use std::future::Future;
use std::num::NonZeroUsize;
use std::pin::pin;
use std::task::{Context, Poll, Waker};

use groove::schema::{ColumnSchema, ColumnType};
use groove::storage::{OrderedKvStorage, ReopenableStorage, RocksDbStorage};

use super::*;
use crate::ids::{AuthorId, BranchId, NodeUuid};
use crate::protocol::{
    BindingViewKey, BranchMetadata, CatalogueAck, KnownStateCompleteness, KnownStateDeclaration,
    LensOp, ReadViewSourceSpec, ReadViewSpec, RegisterShapeOptions, ResultMemberEntry,
    RowVersionRef, ShapeAst, Subscribe, SubscribeRejectReason, SubscribeServerFailureCode,
    TableLens,
};
use crate::protocol_limits::{
    MAX_CONTENT_EXTENT_BYTES, MAX_FETCH_ROW_VERSIONS, MAX_KNOWN_STATE_EXACT_REFS,
    MAX_LOGICAL_MESSAGE_BYTES, MAX_SHAPE_AST_BYTES, MAX_WIRE_FRAME_BYTES,
};
use crate::query::{
    ArraySubquery, BindingId, Include, JoinMode, OrderDirection, PolicyBranch, Predicate,
    RelationOrderBy, ShapeId, all_of, any_of, claim, col, contains, eq, gt, in_list, is_null, lit,
    lte, ne, not, param,
};
use crate::schema::{Policy, TableSchema, WritePolicies};
use crate::time::{GlobalSeq, TxTime};
use crate::tx::TxId;
use crate::wire::decode_sync_message;
use crate::wire::{
    FEATURE_MESSAGE_FRAGMENTATION, FEATURE_STRUCTURED_ERRORS, FEATURE_SYNC_MESSAGE_PAYLOAD,
    WireStreamDecoder, current_wire_features,
};

fn block_on<F: Future>(future: F) -> F::Output {
    let waker = Waker::noop();
    let mut cx = Context::from_waker(waker);
    let mut future = pin!(future);
    loop {
        match future.as_mut().poll(&mut cx) {
            Poll::Ready(value) => return value,
            Poll::Pending => std::thread::yield_now(),
        }
    }
}

fn apply_subscription_event(snapshot: &mut RelationSnapshot, event: SubscriptionEvent) {
    match event {
        SubscriptionEvent::Delta {
            reset,
            added,
            updated,
            removed,
            ..
        } => {
            if reset {
                snapshot.rows.clear();
                snapshot.edges.clear();
                snapshot.root_count = 0;
            }

            for removed in removed {
                if let Some(position) =
                    snapshot
                        .rows
                        .iter()
                        .take(snapshot.root_count)
                        .position(|row| {
                            row.table() == removed.table && row.row_uuid() == removed.row_uuid
                        })
                {
                    snapshot.rows.remove(position);
                    snapshot.root_count -= 1;
                }
            }

            for row in updated {
                let row = row.row;
                if let Some(position) = snapshot.rows.iter().position(|current| {
                    current.table() == row.table() && current.row_uuid() == row.row_uuid()
                }) {
                    snapshot.rows[position] = row;
                }
            }

            for row in added {
                let row = row.row;
                if let Some(position) =
                    snapshot
                        .rows
                        .iter()
                        .take(snapshot.root_count)
                        .position(|current| {
                            current.table() == row.table() && current.row_uuid() == row.row_uuid()
                        })
                {
                    snapshot.rows[position] = row;
                } else {
                    snapshot.rows.insert(snapshot.root_count, row);
                    snapshot.root_count += 1;
                }
            }
        }
        SubscriptionEvent::Rejected { reason } => {
            panic!("unexpected subscription rejection while applying delta: {reason:?}")
        }
        SubscriptionEvent::Closed => {}
    }
}

fn opened_rows(event: SubscriptionEvent) -> Vec<CurrentRow> {
    let mut snapshot = RelationSnapshot::default();
    apply_subscription_event(&mut snapshot, event);
    snapshot.rows
}

fn pending_upstream_subscribe_count<S>(db: &Db<S>) -> usize
where
    S: OrderedKvStorage + ReopenableStorage + 'static,
{
    db.node
        .upstream_subscriptions
        .borrow()
        .iter()
        .filter(|command| matches!(command, PendingUpstreamCommand::Subscribe(_)))
        .count()
}

fn pending_upstream_unsubscribe_count<S>(db: &Db<S>) -> usize
where
    S: OrderedKvStorage + ReopenableStorage + 'static,
{
    db.node
        .upstream_subscriptions
        .borrow()
        .iter()
        .filter(|command| matches!(command, PendingUpstreamCommand::Unsubscribe(_)))
        .count()
}

fn decode_wire_message_payload(
    decoder: &mut WireStreamDecoder,
    envelope: &crate::wire::WireEnvelope,
) -> SyncMessage {
    let payload = decoder
        .decode_message(&envelope.payload, envelope.features)
        .unwrap();
    decode_sync_message(&payload).unwrap()
}

fn delta_rows(event: SubscriptionEvent) -> (Vec<CurrentRow>, Vec<CurrentRow>, Vec<RemovedRow>) {
    match event {
        SubscriptionEvent::Delta {
            added,
            updated,
            removed,
            ..
        } => (
            added.into_iter().map(|output| output.row).collect(),
            updated.into_iter().map(|output| output.row).collect(),
            removed,
        ),
        other => panic!("expected subscription delta event, got {other:?}"),
    }
}

fn snapshot_from_event(event: SubscriptionEvent) -> RelationSnapshot {
    let mut snapshot = RelationSnapshot::default();
    apply_subscription_event(&mut snapshot, event);
    snapshot
}

fn terminal_nested_text_values(
    snapshot: &RelationSnapshot,
    root: RowUuid,
    relation: &str,
    column: &str,
) -> Vec<String> {
    let row = snapshot
        .rows
        .iter()
        .take(snapshot.root_count)
        .find(|row| row.row_uuid() == root)
        .expect("terminal root row");
    let (descriptor, raw) = row.encoded_record();
    let record = groove::records::BorrowedRecord::new(raw, descriptor);
    let Value::Array(children) = record.get(relation).expect("nested terminal field") else {
        panic!("nested terminal field must be an array")
    };
    children
        .into_iter()
        .map(|child| {
            let Value::Record(child) = child else {
                panic!("nested terminal array must contain records")
            };
            let Value::String(value) = child.get(column).expect("nested text field") else {
                panic!("nested terminal field must be text")
            };
            value
        })
        .collect()
}

fn terminal_nested_values(
    snapshot: &RelationSnapshot,
    root: RowUuid,
    relation: &str,
    column: &str,
) -> Vec<Value> {
    let row = snapshot
        .rows
        .iter()
        .take(snapshot.root_count)
        .find(|row| row.row_uuid() == root)
        .expect("terminal root row");
    let (descriptor, raw) = row.encoded_record();
    let record = groove::records::BorrowedRecord::new(raw, descriptor);
    let Value::Array(children) = record.get(relation).expect("nested terminal field") else {
        panic!("nested terminal field must be an array")
    };
    children
        .into_iter()
        .map(|child| {
            let Value::Record(child) = child else {
                panic!("nested terminal array must contain records")
            };
            child.get(column).unwrap_or_else(|error| {
                panic!(
                    "nested field {column:?} missing from {:?}: {error}",
                    child.descriptor()
                )
            })
        })
        .collect()
}

fn oversized_row_version_refs(len: usize) -> Vec<RowVersionRef> {
    (0..len)
        .map(|idx| {
            RowVersionRef::new(
                "todos",
                RowUuid(uuid::Uuid::from_u128(idx as u128 + 1)),
                TxId::new(
                    crate::time::TxTime(idx as u64 + 1),
                    NodeUuid::from_bytes([0x44; 16]),
                ),
            )
        })
        .collect()
}

fn event_settled(event: &SubscriptionEvent) -> bool {
    match event {
        SubscriptionEvent::Delta { settled, .. } => *settled,
        SubscriptionEvent::Rejected { .. } => false,
        SubscriptionEvent::Closed => false,
    }
}

fn global_subscribe_opts() -> ReadOpts {
    ReadOpts {
        tier: DurabilityTier::Global,
        local_updates: LocalUpdates::Deferred,
        propagation: Propagation::Full,
        include_deleted: false,
        ..ReadOpts::default()
    }
}

fn edge_subscribe_opts() -> ReadOpts {
    ReadOpts {
        tier: DurabilityTier::Edge,
        local_updates: LocalUpdates::Deferred,
        propagation: Propagation::Full,
        include_deleted: false,
        ..ReadOpts::default()
    }
}

fn branch_read_opts() -> ReadOpts {
    ReadOpts {
        read_view: ReadViewSpec {
            source: ReadViewSourceSpec::Branch {
                branch: uuid::Uuid::from_bytes([0x42; 16]),
            },
            ..ReadViewSpec::default()
        },
        ..ReadOpts::default()
    }
}

fn assert_unsupported_subscription_include_deleted(error: Error) {
    assert_eq!(error.code, ErrorCode::Query);
    assert!(
        error.message.contains("include_deleted"),
        "unexpected error message: {}",
        error.message
    );
}

fn assert_unsupported_branch_deletion_witness(error: Error) {
    assert!(
        matches!(error.code, ErrorCode::Query | ErrorCode::Protocol),
        "unexpected error code for branch deletion witness gap: {:?}",
        error.code
    );
    assert!(
        error.message.contains("BranchOverlay"),
        "unexpected error message: {}",
        error.message
    );
}

fn assert_subscribe_rejected_branch_overlay(
    message: SyncMessage,
    expected_subscription: SubscriptionKey,
) {
    match message {
        SyncMessage::SubscribeRejected {
            subscription,
            reason: SubscribeRejectReason::UnsupportedShapeCapability { detail },
        } => {
            assert_eq!(subscription, expected_subscription);
            assert!(
                detail.contains("BranchOverlay"),
                "unexpected rejection detail: {detail}"
            );
        }
        other => panic!("expected SubscribeRejected, got {other:?}"),
    }
}

fn assert_subscribe_rejected_unsupported_shape_capability_detail(
    message: SyncMessage,
    expected_subscription: SubscriptionKey,
    expected_detail: &str,
) {
    match message {
        SyncMessage::SubscribeRejected {
            subscription,
            reason: SubscribeRejectReason::UnsupportedShapeCapability { detail },
        } => {
            assert_eq!(subscription, expected_subscription);
            assert!(
                detail.contains(expected_detail),
                "unexpected rejection detail: {detail}"
            );
        }
        other => panic!("expected SubscribeRejected, got {other:?}"),
    }
}

fn assert_view_update_for_subscription(
    message: SyncMessage,
    expected_subscription: SubscriptionKey,
) {
    match message {
        SyncMessage::ViewUpdate { subscription, .. } => {
            assert_eq!(subscription, expected_subscription);
        }
        other => panic!("expected ViewUpdate, got {other:?}"),
    }
}

fn expect_error<T>(result: Result<T, Error>) -> Error {
    match result {
        Ok(_) => panic!("expected operation to fail"),
        Err(error) => error,
    }
}

fn prepared<S>(db: &Db<S>, query: &Query) -> PreparedQuery
where
    S: OrderedKvStorage + ReopenableStorage + 'static,
{
    db.prepare_query(query).unwrap()
}

fn prepared_read<S>(db: &Db<S>, query: &Query) -> Vec<CurrentRow>
where
    S: OrderedKvStorage + ReopenableStorage + 'static,
{
    let prepared = prepared(db, query);
    db.read(&prepared).unwrap()
}

fn prepared_one<S>(db: &Db<S>, query: &Query) -> Option<CurrentRow>
where
    S: OrderedKvStorage + ReopenableStorage + 'static,
{
    let prepared = prepared(db, query);
    db.one(&prepared).unwrap()
}

fn prepared_large_value_cell<S>(
    db: &Db<S>,
    query: &Query,
    table: &TableSchema,
    column: &str,
) -> Vec<u8>
where
    S: OrderedKvStorage + ReopenableStorage + 'static,
{
    let row = prepared_one(db, query).expect("expected one row");
    let Some(Value::Bytes(handle)) = row.cell(table, column) else {
        panic!("expected large-value handle in {column}");
    };
    db.hydrate_large_value_handle(&handle).unwrap()
}

fn prepared_all<S>(db: &Db<S>, query: &Query, opts: ReadOpts) -> Vec<CurrentRow>
where
    S: OrderedKvStorage + ReopenableStorage + 'static,
{
    let prepared = prepared(db, query);
    block_on(db.all(&prepared, opts)).unwrap()
}

fn prepared_subscribe<S>(
    db: &Db<S>,
    query: &Query,
    opts: ReadOpts,
) -> Result<SubscriptionStream, Error>
where
    S: OrderedKvStorage + ReopenableStorage + 'static,
{
    let prepared = prepared(db, query);
    block_on(db.subscribe(&prepared, opts))
}

#[derive(Default)]
struct RecordingScheduler {
    calls: RefCell<Vec<TickUrgency>>,
}

impl TickScheduler for RecordingScheduler {
    fn schedule_tick(&self, urgency: TickUrgency) {
        self.calls.borrow_mut().push(urgency);
    }
}

impl RecordingScheduler {
    fn take(&self) -> Vec<TickUrgency> {
        std::mem::take(&mut self.calls.borrow_mut())
    }
}

fn schema() -> JazzSchema {
    JazzSchema::new([TableSchema::new(
        "todos",
        [
            ColumnSchema::new("title", ColumnType::String),
            ColumnSchema::new("done", ColumnType::Bool),
            ColumnSchema::new("owner", ColumnType::Uuid),
        ],
    )
    .with_read_policy(Policy::public())
    .with_write_policy(Policy::public())])
}

fn owner_read_schema() -> JazzSchema {
    JazzSchema::new([TableSchema::new(
        "todos",
        [
            ColumnSchema::new("title", ColumnType::String),
            ColumnSchema::new("done", ColumnType::Bool),
            ColumnSchema::new("owner", ColumnType::Uuid),
        ],
    )
    .with_read_policy(Policy::owner_only("todos", "owner"))
    .with_write_policy(Policy::public())])
}

fn created_by_read_schema() -> JazzSchema {
    created_by_read_schema_for_claim("sub")
}

fn created_by_read_schema_for_claim(claim_name: &str) -> JazzSchema {
    JazzSchema::new([TableSchema::new(
        "todos",
        [
            ColumnSchema::new("title", ColumnType::String),
            ColumnSchema::new("done", ColumnType::Bool),
        ],
    )
    .with_read_policy(Policy::shape(
        Query::from("todos").filter(eq(col("$createdBy"), claim(claim_name))),
    ))
    .with_write_policy(Policy::public())])
}

fn owner_write_schema() -> JazzSchema {
    JazzSchema::new([TableSchema::new(
        "todos",
        [
            ColumnSchema::new("title", ColumnType::String),
            ColumnSchema::new("done", ColumnType::Bool),
            ColumnSchema::new("owner", ColumnType::Uuid),
        ],
    )
    .with_read_policy(Policy::public())
    .with_write_policy(Policy::owner_only("todos", "owner"))])
}

fn editor_claim_write_schema() -> JazzSchema {
    JazzSchema::new([TableSchema::new(
        "todos",
        [
            ColumnSchema::new("title", ColumnType::String),
            ColumnSchema::new("done", ColumnType::Bool),
            ColumnSchema::new("owner", ColumnType::Uuid),
        ],
    )
    .with_read_policy(Policy::public())
    .with_write_policy(Policy::shape(
        Query::from("todos").filter(eq(claim("role"), lit("editor"))),
    ))])
}

fn owner_id_read_schema() -> JazzSchema {
    JazzSchema::new([TableSchema::new(
        "messages",
        [
            ColumnSchema::new("body", ColumnType::String),
            ColumnSchema::new("owner_id", ColumnType::String),
        ],
    )
    .with_read_policy(Policy::shape(
        Query::from("messages").filter(eq(col("owner_id"), crate::query::claim("user_id"))),
    ))
    .with_write_policy(Policy::public())])
}

fn owner_id_public_schema() -> JazzSchema {
    JazzSchema::new([TableSchema::new(
        "messages",
        [
            ColumnSchema::new("body", ColumnType::String),
            ColumnSchema::new("owner_id", ColumnType::String),
        ],
    )
    .with_read_policy(Policy::public())
    .with_write_policy(Policy::public())])
}

fn benchmark_shaped_recursive_reachable_read_schema() -> JazzSchema {
    let resource_policy = Policy::shape(
        Query::from("res_a")
            .reachable_via_with_access_filters(
                "res_a_access_edges",
                "resource",
                "team",
                lit("relation-seeded"),
                [eq(col("administrator"), lit(false))],
                "group_entry",
                "member_id",
                "target_id",
                [eq(col("administrator"), lit(false))],
            )
            .seeded_by("group_access_edges", "user_id", "sub", "group_id"),
    );

    JazzSchema::new([
        TableSchema::new(
            "res_a",
            [
                ColumnSchema::new("org_id", ColumnType::Uuid),
                ColumnSchema::new("created_by", ColumnType::Uuid),
                ColumnSchema::new("updated_by", ColumnType::Uuid),
                ColumnSchema::new("archived", ColumnType::Bool),
                ColumnSchema::new("label", ColumnType::String),
                ColumnSchema::new("date_created", ColumnType::U64),
                ColumnSchema::new("date_updated", ColumnType::U64),
                ColumnSchema::new("col_text_a", ColumnType::String.nullable()),
                ColumnSchema::new("col_text_b", ColumnType::String.nullable()),
                ColumnSchema::new("col_float", ColumnType::F64.nullable()),
                ColumnSchema::new("col_int", ColumnType::U64.nullable()),
                ColumnSchema::new("col_json", ColumnType::String.nullable()),
                ColumnSchema::new("col_tags", ColumnType::String.nullable()),
            ],
        )
        .with_reference("created_by", "group")
        .with_reference("updated_by", "group")
        .with_read_policy(resource_policy)
        .with_write_policy(Policy::public()),
        TableSchema::new("group", [ColumnSchema::new("name", ColumnType::String)])
            .with_read_policy(Policy::public())
            .with_write_policy(Policy::public()),
        TableSchema::new(
            "group_access_edges",
            [
                ColumnSchema::new("group_id", ColumnType::Uuid),
                ColumnSchema::new("user_id", ColumnType::Uuid),
                ColumnSchema::new("role", ColumnType::String),
            ],
        )
        .with_reference("group_id", "group")
        .with_read_policy(Policy::public())
        .with_write_policy(Policy::public()),
        TableSchema::new(
            "res_a_access_edges",
            [
                ColumnSchema::new("resource", ColumnType::Uuid),
                ColumnSchema::new("team", ColumnType::Uuid),
                ColumnSchema::new("grant_role", ColumnType::String),
                ColumnSchema::new("administrator", ColumnType::Bool),
            ],
        )
        .with_reference("resource", "res_a")
        .with_reference("team", "group")
        .with_read_policy(Policy::public())
        .with_write_policy(Policy::public()),
        TableSchema::new(
            "group_entry",
            [
                ColumnSchema::new("member_id", ColumnType::Uuid),
                ColumnSchema::new("target_id", ColumnType::Uuid),
                ColumnSchema::new("administrator", ColumnType::Bool),
                ColumnSchema::new("date_added", ColumnType::U64),
            ],
        )
        .with_reference("member_id", "group")
        .with_reference("target_id", "group")
        .with_write_policy(Policy::public()),
    ])
}

fn customer_resource_policy_minimal_schema() -> JazzSchema {
    let resource_policy = Policy::shape(
        Query::from("res_i")
            .reachable_via_with_access_filters(
                "res_i_access_edges",
                "resource",
                "team",
                lit("relation-seeded"),
                [eq(col("administrator"), lit(false))],
                "group_entry",
                "member_id",
                "target_id",
                [eq(col("administrator"), lit(false))],
            )
            .seeded_by("group_access_edges", "user_id", "sub", "group_id"),
    );

    JazzSchema::new([
        TableSchema::new("org", [ColumnSchema::new("label", ColumnType::String)])
            .with_read_policy(Policy::public())
            .with_write_policy(Policy::public()),
        TableSchema::new("group", [ColumnSchema::new("name", ColumnType::String)])
            .with_read_policy(Policy::public())
            .with_write_policy(Policy::public()),
        TableSchema::new(
            "group_access_edges",
            [
                ColumnSchema::new("group_id", ColumnType::Uuid),
                ColumnSchema::new("user_id", ColumnType::Uuid),
                ColumnSchema::new("role", ColumnType::String),
            ],
        )
        .with_reference("group_id", "group")
        .with_read_policy(Policy::public())
        .with_write_policy(Policy::public()),
        TableSchema::new(
            "group_entry",
            [
                ColumnSchema::new("member_id", ColumnType::Uuid),
                ColumnSchema::new("target_id", ColumnType::Uuid),
                ColumnSchema::new("administrator", ColumnType::Bool),
                ColumnSchema::new("date_added", ColumnType::U64),
            ],
        )
        .with_reference("member_id", "group")
        .with_reference("target_id", "group")
        .with_read_policy(Policy::public())
        .with_write_policy(Policy::public()),
        TableSchema::new("res_i", resource_columns_for_customer_fixture())
            .with_reference("org_id", "org")
            .with_reference("created_by", "group")
            .with_reference("updated_by", "group")
            .with_read_policy(resource_policy)
            .with_write_policy(Policy::public()),
        TableSchema::new(
            "res_i_access_edges",
            [
                ColumnSchema::new("resource", ColumnType::Uuid),
                ColumnSchema::new("team", ColumnType::Uuid),
                ColumnSchema::new("grant_role", ColumnType::String),
                ColumnSchema::new("administrator", ColumnType::Bool),
            ],
        )
        .with_reference("resource", "res_i")
        .with_reference("team", "group")
        .with_read_policy(Policy::public())
        .with_write_policy(Policy::public()),
    ])
}

fn customer_two_resource_policy_minimal_schema() -> JazzSchema {
    let res_i_policy = Policy::shape(
        Query::from("res_i")
            .reachable_via_with_access_filters(
                "res_i_access_edges",
                "resource",
                "team",
                lit("relation-seeded"),
                [eq(col("administrator"), lit(false))],
                "group_entry",
                "member_id",
                "target_id",
                [eq(col("administrator"), lit(false))],
            )
            .seeded_by("group_access_edges", "user_id", "sub", "group_id"),
    );
    let res_j_policy = Policy::shape(
        Query::from("res_j")
            .reachable_via_with_access_filters(
                "res_j_access_edges",
                "resource",
                "team",
                lit("relation-seeded"),
                [eq(col("administrator"), lit(false))],
                "group_entry",
                "member_id",
                "target_id",
                [eq(col("administrator"), lit(false))],
            )
            .seeded_by("group_access_edges", "user_id", "sub", "group_id"),
    );

    JazzSchema::new([
        TableSchema::new("org", [ColumnSchema::new("label", ColumnType::String)])
            .with_read_policy(Policy::public())
            .with_write_policy(Policy::public()),
        TableSchema::new("group", [ColumnSchema::new("name", ColumnType::String)])
            .with_read_policy(Policy::public())
            .with_write_policy(Policy::public()),
        TableSchema::new(
            "group_access_edges",
            [
                ColumnSchema::new("group_id", ColumnType::Uuid),
                ColumnSchema::new("user_id", ColumnType::Uuid),
                ColumnSchema::new("role", ColumnType::String),
            ],
        )
        .with_reference("group_id", "group")
        .with_read_policy(Policy::public())
        .with_write_policy(Policy::public()),
        TableSchema::new(
            "group_entry",
            [
                ColumnSchema::new("member_id", ColumnType::Uuid),
                ColumnSchema::new("target_id", ColumnType::Uuid),
                ColumnSchema::new("administrator", ColumnType::Bool),
                ColumnSchema::new("date_added", ColumnType::U64),
            ],
        )
        .with_reference("member_id", "group")
        .with_reference("target_id", "group")
        .with_read_policy(Policy::public())
        .with_write_policy(Policy::public()),
        TableSchema::new("res_i", resource_columns_for_customer_fixture())
            .with_reference("org_id", "org")
            .with_reference("created_by", "group")
            .with_reference("updated_by", "group")
            .with_read_policy(res_i_policy)
            .with_write_policy(Policy::public()),
        TableSchema::new(
            "res_i_access_edges",
            [
                ColumnSchema::new("resource", ColumnType::Uuid),
                ColumnSchema::new("team", ColumnType::Uuid),
                ColumnSchema::new("grant_role", ColumnType::String),
                ColumnSchema::new("administrator", ColumnType::Bool),
            ],
        )
        .with_reference("resource", "res_i")
        .with_reference("team", "group")
        .with_read_policy(Policy::public())
        .with_write_policy(Policy::public()),
        TableSchema::new("res_j", resource_columns_for_customer_fixture())
            .with_reference("org_id", "org")
            .with_reference("created_by", "group")
            .with_reference("updated_by", "group")
            .with_read_policy(res_j_policy)
            .with_write_policy(Policy::public()),
        TableSchema::new(
            "res_j_access_edges",
            [
                ColumnSchema::new("resource", ColumnType::Uuid),
                ColumnSchema::new("team", ColumnType::Uuid),
                ColumnSchema::new("grant_role", ColumnType::String),
                ColumnSchema::new("administrator", ColumnType::Bool),
            ],
        )
        .with_reference("resource", "res_j")
        .with_reference("team", "group")
        .with_read_policy(Policy::public())
        .with_write_policy(Policy::public()),
    ])
}

fn same_table_seeded_resource_policy_schema() -> JazzSchema {
    let resource_policy = Policy::shape(
        Query::from("resources")
            .reachable_via_with_access_filters(
                "resource_access",
                "resource",
                "team",
                lit("relation-seeded"),
                [eq(col("administrator"), lit(false))],
                "team_entries",
                "member_id",
                "target_id",
                [eq(col("administrator"), lit(false))],
            )
            .seeded_by("teams", "identity_key", "sub", "id"),
    );

    JazzSchema::new([
        TableSchema::new(
            "teams",
            [
                ColumnSchema::new("name", ColumnType::String),
                ColumnSchema::new("identity_key", ColumnType::Uuid),
            ],
        )
        .with_read_policy(Policy::public())
        .with_write_policy(Policy::public()),
        TableSchema::new(
            "team_entries",
            [
                ColumnSchema::new("member_id", ColumnType::Uuid),
                ColumnSchema::new("target_id", ColumnType::Uuid),
                ColumnSchema::new("administrator", ColumnType::Bool),
            ],
        )
        .with_reference("member_id", "teams")
        .with_reference("target_id", "teams")
        .with_read_policy(Policy::public())
        .with_write_policy(Policy::public()),
        TableSchema::new(
            "resources",
            [ColumnSchema::new("label", ColumnType::String)],
        )
        .with_read_policy(resource_policy)
        .with_write_policy(Policy::public()),
        TableSchema::new(
            "resource_access",
            [
                ColumnSchema::new("resource", ColumnType::Uuid),
                ColumnSchema::new("team", ColumnType::Uuid),
                ColumnSchema::new("administrator", ColumnType::Bool),
            ],
        )
        .with_reference("resource", "resources")
        .with_reference("team", "teams")
        .with_read_policy(Policy::public())
        .with_write_policy(Policy::public()),
    ])
}

fn same_table_string_seeded_resource_policy_schema() -> JazzSchema {
    let resource_policy = Policy::shape(
        Query::from("resources")
            .reachable_via_with_access_filters(
                "resource_access",
                "resource",
                "team",
                lit("relation-seeded"),
                [eq(col("administrator"), lit(false))],
                "team_entries",
                "member_id",
                "target_id",
                [eq(col("administrator"), lit(false))],
            )
            .seeded_by("teams", "identity_key", "user_id", "id"),
    );

    JazzSchema::new([
        TableSchema::new(
            "teams",
            [
                ColumnSchema::new("name", ColumnType::String),
                ColumnSchema::new("identity_key", ColumnType::String),
            ],
        )
        .with_read_policy(Policy::public())
        .with_write_policy(Policy::public()),
        TableSchema::new(
            "team_entries",
            [
                ColumnSchema::new("member_id", ColumnType::Uuid),
                ColumnSchema::new("target_id", ColumnType::Uuid),
                ColumnSchema::new("administrator", ColumnType::Bool),
            ],
        )
        .with_reference("member_id", "teams")
        .with_reference("target_id", "teams")
        .with_read_policy(Policy::public())
        .with_write_policy(Policy::public()),
        TableSchema::new(
            "resources",
            [ColumnSchema::new("label", ColumnType::String)],
        )
        .with_read_policy(resource_policy)
        .with_write_policy(Policy::public()),
        TableSchema::new(
            "resource_access",
            [
                ColumnSchema::new("resource", ColumnType::Uuid),
                ColumnSchema::new("team", ColumnType::Uuid),
                ColumnSchema::new("administrator", ColumnType::Bool),
            ],
        )
        .with_reference("resource", "resources")
        .with_reference("team", "teams")
        .with_read_policy(Policy::public())
        .with_write_policy(Policy::public()),
    ])
}

fn customer_inherited_child_policy_schema() -> JazzSchema {
    let resource_policy = Query::from("res_i")
        .reachable_via_with_access_filters(
            "res_i_access_edges",
            "resource",
            "team",
            lit("relation-seeded"),
            [eq(col("administrator"), lit(false))],
            "group_entry",
            "member_id",
            "target_id",
            [eq(col("administrator"), lit(false))],
        )
        .seeded_by("group_access_edges", "user_id", "sub", "group_id");
    JazzSchema::new([
        TableSchema::new("org", [ColumnSchema::new("label", ColumnType::String)])
            .with_read_policy(Policy::public())
            .with_write_policy(Policy::public()),
        TableSchema::new("group", [ColumnSchema::new("name", ColumnType::String)])
            .with_read_policy(Policy::public())
            .with_write_policy(Policy::public()),
        TableSchema::new(
            "group_access_edges",
            [
                ColumnSchema::new("group_id", ColumnType::Uuid),
                ColumnSchema::new("user_id", ColumnType::Uuid),
                ColumnSchema::new("role", ColumnType::String),
            ],
        )
        .with_reference("group_id", "group")
        .with_read_policy(Policy::public())
        .with_write_policy(Policy::public()),
        TableSchema::new(
            "group_entry",
            [
                ColumnSchema::new("member_id", ColumnType::Uuid),
                ColumnSchema::new("target_id", ColumnType::Uuid),
                ColumnSchema::new("administrator", ColumnType::Bool),
                ColumnSchema::new("date_added", ColumnType::U64),
            ],
        )
        .with_reference("member_id", "group")
        .with_reference("target_id", "group")
        .with_read_policy(Policy::public())
        .with_write_policy(Policy::public()),
        TableSchema::new("res_i", resource_columns_for_customer_fixture())
            .with_reference("org_id", "org")
            .with_reference("created_by", "group")
            .with_reference("updated_by", "group")
            .with_read_policy(Policy::shape(resource_policy))
            .with_write_policy(Policy::public()),
        TableSchema::new(
            "res_i_access_edges",
            [
                ColumnSchema::new("resource", ColumnType::Uuid),
                ColumnSchema::new("team", ColumnType::Uuid),
                ColumnSchema::new("grant_role", ColumnType::String),
                ColumnSchema::new("administrator", ColumnType::Bool),
            ],
        )
        .with_reference("resource", "res_i")
        .with_reference("team", "group")
        .with_read_policy(Policy::public())
        .with_write_policy(Policy::public()),
        TableSchema::new(
            "res_i_child",
            [
                ColumnSchema::new("resource", ColumnType::Uuid),
                ColumnSchema::new("status", ColumnType::String),
                ColumnSchema::new("label", ColumnType::String),
            ],
        )
        .with_reference("resource", "res_i")
        .with_read_policy(Policy::shape(
            Query::from("res_i_child")
                .inherits("resource")
                .filter(eq(col("status"), lit("open"))),
        ))
        .with_write_policy(Policy::public()),
        TableSchema::new(
            "res_i_grandchild",
            [
                ColumnSchema::new("child", ColumnType::Uuid),
                ColumnSchema::new("label", ColumnType::String),
            ],
        )
        .with_reference("child", "res_i_child")
        .with_read_policy(Policy::shape(
            Query::from("res_i_grandchild").inherits("child"),
        ))
        .with_write_policy(Policy::public()),
    ])
}

fn inherited_insert_policy_schema() -> JazzSchema {
    let parent_update_using = Query::from("parents").filter(eq(col("owner"), claim("sub")));
    let parent_update_check = Query::from("parents").filter(eq(col("locked"), lit(false)));
    JazzSchema::new([
        TableSchema::new(
            "parents",
            [
                ColumnSchema::new("owner", ColumnType::Uuid),
                ColumnSchema::new("locked", ColumnType::Bool),
            ],
        )
        .with_read_policy(Policy::public())
        .with_write_policies(WritePolicies {
            insert_check: Policy::public(),
            update_using: Some(parent_update_using),
            update_check: Some(parent_update_check),
            delete_using: None,
        }),
        TableSchema::new(
            "children",
            [
                ColumnSchema::new("parent_id", ColumnType::Uuid),
                ColumnSchema::new("label", ColumnType::String),
            ],
        )
        .with_reference("parent_id", "parents")
        .with_read_policy(Policy::public())
        .with_write_policies(WritePolicies {
            insert_check: Some(Query::from("children").inherits("parent_id")),
            update_using: None,
            update_check: None,
            delete_using: None,
        }),
    ])
}

fn resource_columns_for_customer_fixture() -> [ColumnSchema; 13] {
    [
        ColumnSchema::new("org_id", ColumnType::Uuid),
        ColumnSchema::new("created_by", ColumnType::Uuid),
        ColumnSchema::new("updated_by", ColumnType::Uuid),
        ColumnSchema::new("archived", ColumnType::Bool),
        ColumnSchema::new("label", ColumnType::String),
        ColumnSchema::new("date_created", ColumnType::U64),
        ColumnSchema::new("date_updated", ColumnType::U64),
        ColumnSchema::new("col_text_a", ColumnType::String.nullable()),
        ColumnSchema::new("col_text_b", ColumnType::String.nullable()),
        ColumnSchema::new("col_float", ColumnType::F64.nullable()),
        ColumnSchema::new("col_int", ColumnType::U64.nullable()),
        ColumnSchema::new("col_json", ColumnType::String.nullable()),
        ColumnSchema::new("col_tags", ColumnType::String.nullable()),
    ]
}

fn owner_blob_schema() -> JazzSchema {
    JazzSchema::new([TableSchema::new(
        "assets",
        [
            crate::schema::ColumnSchema::new("owner", ColumnType::Uuid),
            crate::schema::ColumnSchema::new("mime_type", ColumnType::String),
            crate::schema::ColumnSchema::blob("data"),
        ],
    )
    .with_read_policy(Policy::owner_only("assets", "owner"))
    .with_write_policy(Policy::owner_only("assets", "owner"))])
}

fn relation_schema() -> JazzSchema {
    JazzSchema::new([
        TableSchema::new("users", [ColumnSchema::new("name", ColumnType::String)])
            .with_read_policy(Policy::public())
            .with_write_policy(Policy::public()),
        TableSchema::new(
            "todos",
            [
                ColumnSchema::new("title", ColumnType::String),
                ColumnSchema::new("owner_id", ColumnType::Uuid),
            ],
        )
        .with_reference("owner_id", "users")
        .with_read_policy(Policy::public())
        .with_write_policy(Policy::public()),
        TableSchema::new(
            "comments",
            [
                ColumnSchema::new("body", ColumnType::String),
                ColumnSchema::new("todo_id", ColumnType::Uuid),
            ],
        )
        .with_reference("todo_id", "todos")
        .with_read_policy(Policy::public())
        .with_write_policy(Policy::public()),
    ])
}

fn relation_hop_schema() -> JazzSchema {
    JazzSchema::new([
        TableSchema::new("orgs", [ColumnSchema::new("name", ColumnType::String)])
            .with_read_policy(Policy::public())
            .with_write_policy(Policy::public()),
        TableSchema::new(
            "teams",
            [
                ColumnSchema::new("name", ColumnType::String),
                ColumnSchema::new("org_id", ColumnType::Nullable(Box::new(ColumnType::Uuid))),
            ],
        )
        .with_reference("org_id", "orgs")
        .with_read_policy(Policy::public())
        .with_write_policy(Policy::public()),
        TableSchema::new(
            "users",
            [
                ColumnSchema::new("name", ColumnType::String),
                ColumnSchema::new("team_id", ColumnType::Nullable(Box::new(ColumnType::Uuid))),
            ],
        )
        .with_reference("team_id", "teams")
        .with_read_policy(Policy::public())
        .with_write_policy(Policy::public()),
    ])
}

fn access_edge_include_schema() -> JazzSchema {
    JazzSchema::new([
        TableSchema::new("teams", [ColumnSchema::new("name", ColumnType::String)])
            .with_read_policy(Policy::public())
            .with_write_policy(Policy::public()),
        TableSchema::new(
            "team_access_edges",
            [
                ColumnSchema::new("resource_id", ColumnType::Uuid),
                ColumnSchema::new("team_id", ColumnType::Uuid),
            ],
        )
        .with_reference("resource_id", "teams")
        .with_reference("team_id", "teams")
        .with_read_policy(Policy::public())
        .with_write_policy(Policy::public()),
    ])
}

fn policy_relation_schema() -> JazzSchema {
    JazzSchema::new([
        TableSchema::new("todos", [ColumnSchema::new("title", ColumnType::String)])
            .with_read_policy(Policy::public())
            .with_write_policy(Policy::public()),
        TableSchema::new(
            "comments",
            [
                ColumnSchema::new("body", ColumnType::String),
                ColumnSchema::new("todo_id", ColumnType::Uuid),
                ColumnSchema::new("owner", ColumnType::Uuid),
            ],
        )
        .with_read_policy(Policy::owner_only("comments", "owner"))
        .with_write_policy(Policy::public()),
    ])
}

fn evolved_owner_write_schema() -> JazzSchema {
    JazzSchema::new([TableSchema::new(
        "todos",
        [
            ColumnSchema::new("title", ColumnType::String),
            ColumnSchema::new("done", ColumnType::Bool),
            ColumnSchema::new("owner", ColumnType::Uuid),
            ColumnSchema::new("body", ColumnType::String),
        ],
    )
    .with_read_policy(Policy::public())
    .with_write_policy(Policy::owner_only("todos", "owner"))])
}

fn row(byte: u8) -> RowUuid {
    RowUuid::from_bytes([byte; 16])
}

#[test]
fn view_update_is_not_empty_when_it_only_carries_program_facts() {
    let subscription = crate::protocol::SubscriptionKey {
        shape_id: crate::query::ShapeId(uuid::Uuid::from_bytes([0x11; 16])),
        binding_id: crate::query::BindingId(uuid::Uuid::from_bytes([0x22; 16])),
        read_view: Default::default(),
    };
    let empty = SyncMessage::ViewUpdate {
        subscription,
        settled_through: crate::time::GlobalSeq(0),
        reset_result_set: false,
        version_carriers: Vec::new(),
        version_bundles: Vec::new(),
        peer_payload_inventory: crate::protocol::PeerPayloadInventory::default(),
        result_member_adds: Vec::new(),
        result_member_removes: Vec::new(),
        terminal_operations: Vec::new(),
        program_fact_adds: Vec::new(),
        program_fact_removes: Vec::new(),
    };
    assert!(view_update_is_empty(&empty));

    let fact_only = SyncMessage::ViewUpdate {
        subscription,
        settled_through: crate::time::GlobalSeq(0),
        reset_result_set: false,
        version_carriers: Vec::new(),
        version_bundles: Vec::new(),
        peer_payload_inventory: crate::protocol::PeerPayloadInventory::default(),
        result_member_adds: Vec::new(),
        result_member_removes: Vec::new(),
        terminal_operations: Vec::new(),
        program_fact_adds: vec![crate::protocol::ViewFactEntry::PathCorrelationCoverage(
            crate::protocol::PathCorrelationCoverageEntry {
                path: "owner".to_owned(),
                source_table: "todos".to_owned().into(),
                source_row: row(1),
                correlation_key: vec![1],
                complete: true,
            },
        )],
        program_fact_removes: Vec::new(),
    };
    assert!(!view_update_is_empty(&fact_only));
}

fn cells(title: &str, done: bool, owner: AuthorId) -> RowCells {
    BTreeMap::from([
        ("title".to_owned(), Value::String(title.to_owned())),
        ("done".to_owned(), Value::Bool(done)),
        ("owner".to_owned(), Value::Uuid(owner.0)),
    ])
}

fn issue_schema() -> JazzSchema {
    JazzSchema::new([
        TableSchema::new("projects", [ColumnSchema::new("name", ColumnType::String)])
            .with_read_policy(Policy::public())
            .with_write_policy(Policy::public()),
        TableSchema::new(
            "issues",
            [
                ColumnSchema::new("title", ColumnType::String),
                ColumnSchema::new("state", ColumnType::String),
                ColumnSchema::new("assignee", ColumnType::Uuid),
                ColumnSchema::new("project", ColumnType::Uuid),
                ColumnSchema::new("priority", ColumnType::U64),
                ColumnSchema::new("labels", ColumnType::String.array_of()),
                ColumnSchema::new("snoozed_until", ColumnType::U64.nullable()),
            ],
        )
        .with_reference("project", "projects")
        .with_read_policy(Policy::public())
        .with_write_policy(Policy::public()),
        TableSchema::new(
            "issue_tags",
            [
                ColumnSchema::new("issue", ColumnType::Uuid),
                ColumnSchema::new("tag", ColumnType::String),
            ],
        )
        .with_reference("issue", "issues")
        .with_read_policy(Policy::public())
        .with_write_policy(Policy::public()),
    ])
}

fn issue_cells(
    title: &str,
    state: &str,
    assignee: AuthorId,
    project: RowUuid,
    priority: u64,
    labels: &[&str],
    snoozed_until: Option<u64>,
) -> RowCells {
    BTreeMap::from([
        ("title".to_owned(), Value::String(title.to_owned())),
        ("state".to_owned(), Value::String(state.to_owned())),
        ("assignee".to_owned(), Value::Uuid(assignee.0)),
        ("project".to_owned(), Value::Uuid(project.0)),
        ("priority".to_owned(), Value::U64(priority)),
        (
            "labels".to_owned(),
            Value::Array(
                labels
                    .iter()
                    .map(|label| Value::String((*label).to_owned()))
                    .collect(),
            ),
        ),
        (
            "snoozed_until".to_owned(),
            Value::Nullable(snoozed_until.map(|value| Box::new(Value::U64(value)))),
        ),
    ])
}

#[test]
fn client_insert_advice_is_unknown_without_writing() {
    let schema = owner_write_schema();
    let owner = AuthorId::from_bytes([0xa1; 16]);
    let other = AuthorId::from_bytes([0xb2; 16]);
    let owner_db = open_db(0xa1, owner, &schema);
    let other_db = open_db(0xb2, other, &schema);

    assert_eq!(
        owner_db
            .can_insert("todos", cells("owned", false, owner))
            .unwrap(),
        PermissionAdvice::Unknown,
    );
    assert_eq!(
        other_db
            .can_insert("todos", cells("owned", false, owner))
            .unwrap(),
        PermissionAdvice::Unknown,
    );
    assert_eq!(
        owner_db
            .authorize_insert_for_identity("todos", cells("owned", false, owner), owner)
            .unwrap(),
        PermissionAdvice::Allowed,
    );
    assert_eq!(
        owner_db
            .authorize_insert_for_identity("todos", cells("owned", false, owner), other)
            .unwrap(),
        PermissionAdvice::Denied,
    );
    assert_eq!(prepared_read(&owner_db, &owner_db.table("todos")).len(), 0);
    assert_eq!(prepared_read(&other_db, &other_db.table("todos")).len(), 0);
}

#[test]
fn client_read_advice_is_unknown_even_when_a_local_winner_exists() {
    let schema = owner_read_schema();
    let owner = AuthorId::from_bytes([0xa1; 16]);
    let other = AuthorId::from_bytes([0xb2; 16]);
    let core = open_core(0x5e, AuthorId::SYSTEM, &schema);
    let row = row(1);
    let write = core
        .insert_with_id("todos", row, cells("private", false, owner))
        .unwrap();

    let owner_db = open_db(0xa1, owner, &schema);
    let other_db = open_db(0xb2, other, &schema);
    let unit = core
        .node()
        .borrow_mut()
        .commit_unit_for(write.mergeable_tx_id());
    let SyncMessage::CommitUnit { tx, versions } = unit.unwrap() else {
        panic!("commit unit expected");
    };
    owner_db
        .node
        .node
        .borrow_mut()
        .apply_sync_message(SyncMessage::CommitUnit {
            tx: tx.clone(),
            versions: versions.clone(),
        })
        .unwrap();
    other_db
        .node
        .node
        .borrow_mut()
        .apply_sync_message(SyncMessage::CommitUnit { tx, versions })
        .unwrap();

    assert_eq!(
        owner_db.can_read("todos", row).unwrap(),
        PermissionAdvice::Unknown
    );
    assert_eq!(
        other_db.can_read("todos", row).unwrap(),
        PermissionAdvice::Unknown
    );
    assert_eq!(
        owner_db
            .authorize_read_for_identity("todos", row, owner)
            .unwrap(),
        PermissionAdvice::Allowed,
    );
    assert_eq!(
        owner_db
            .authorize_read_for_identity("todos", row, other)
            .unwrap(),
        PermissionAdvice::Denied,
    );
}

#[test]
fn client_delete_advice_is_unknown_without_mutating() {
    let schema = owner_write_schema();
    let owner = AuthorId::from_bytes([0xa1; 16]);
    let other = AuthorId::from_bytes([0xb2; 16]);
    let owner_db = open_db(0xa1, owner, &schema);
    let other_db = open_db(0xb2, other, &schema);
    let row = row(1);
    let write = owner_db
        .insert_with_id("todos", row, cells("owned", false, owner))
        .unwrap();
    other_db
        .node
        .node
        .borrow_mut()
        .apply_sync_message(
            owner_db
                .node
                .node
                .borrow_mut()
                .commit_unit_for(write.mergeable_tx_id())
                .unwrap(),
        )
        .unwrap();

    assert_eq!(
        owner_db.can_delete("todos", row).unwrap(),
        PermissionAdvice::Unknown
    );
    assert_eq!(
        other_db.can_delete("todos", row).unwrap(),
        PermissionAdvice::Unknown
    );
    assert_eq!(
        owner_db
            .authorize_delete_for_identity("todos", row, owner)
            .unwrap(),
        PermissionAdvice::Allowed,
    );
    assert_eq!(
        owner_db
            .authorize_delete_for_identity("todos", row, other)
            .unwrap(),
        PermissionAdvice::Denied,
    );
    assert_eq!(prepared_read(&owner_db, &owner_db.table("todos")).len(), 1);
    assert_eq!(prepared_read(&other_db, &other_db.table("todos")).len(), 1);
}

#[test]
fn core_attributed_insert_uses_core_identity_for_policy_and_user_for_made_by() {
    let schema = owner_write_schema();
    let backend = AuthorId::from_bytes([0xbe; 16]);
    let attributed_user = AuthorId::from_bytes([0xa1; 16]);
    let core = open_core(0x5e, backend, &schema);
    let write = core
        .insert_attributed(
            attributed_user,
            "todos",
            cells("attributed", false, backend),
        )
        .unwrap();

    let unit = core
        .node()
        .borrow_mut()
        .commit_unit_for(write.mergeable_tx_id())
        .unwrap();
    let SyncMessage::CommitUnit { tx, .. } = unit else {
        panic!("commit unit expected");
    };

    assert_eq!(tx.made_by, attributed_user);
    assert_eq!(core.read(&core.table("todos")).unwrap().len(), 1);
}

#[test]
fn client_attributed_insert_to_different_user_is_rejected() {
    let schema = owner_write_schema();
    let client_author = AuthorId::from_bytes([0xc1; 16]);
    let attributed_user = AuthorId::from_bytes([0xa1; 16]);
    let client = open_db(0xc1, client_author, &schema);

    let err = match client.insert_attributed(
        attributed_user,
        "todos",
        cells("forged", false, client_author),
    ) {
        Ok(_) => panic!("client attribution should be rejected"),
        Err(err) => err,
    };

    assert_eq!(err.code, ErrorCode::WriteRejected);
    assert_eq!(prepared_read(&client, &client.table("todos")).len(), 0);
}

#[test]
fn default_insert_keeps_subject_and_made_by_equal() {
    let schema = owner_write_schema();
    let owner = AuthorId::from_bytes([0xa1; 16]);
    let db = open_db(0xa1, owner, &schema);
    let write = db.insert("todos", cells("default", false, owner)).unwrap();
    let unit = db
        .node
        .node
        .borrow_mut()
        .commit_unit_for(write.mergeable_tx_id())
        .unwrap();
    let SyncMessage::CommitUnit { tx, .. } = unit else {
        panic!("commit unit expected");
    };

    assert_eq!(tx.made_by, owner);
    assert_eq!(prepared_read(&db, &db.table("todos")).len(), 1);
}

#[test]
fn db_facade_opens_writes_and_reads_todos_end_to_end() {
    let db = doctest_support::block_on(doctest_support::open_todos_db()).unwrap();
    let write = db
        .insert(
            "todos",
            doctest_support::todo_cells("learn the db facade", false),
        )
        .unwrap();
    let todo = write.row_uuid();
    doctest_support::block_on(write.wait(DurabilityTier::Local)).unwrap();

    let query = db.table("todos");
    let table = &doctest_support::schema().tables[0];

    let read_rows = prepared_read(&db, &query);
    assert_eq!(row_ids(&read_rows), vec![todo]);
    assert_eq!(
        read_rows[0].cell(table, "title"),
        Some(Value::String("learn the db facade".to_owned()))
    );
    assert_eq!(read_rows[0].cell(table, "done"), Some(Value::Bool(false)));

    let one_row = prepared_one(&db, &query).unwrap();
    assert_eq!(one_row.row_uuid(), todo);
    assert_eq!(
        one_row.cell(table, "title"),
        Some(Value::String("learn the db facade".to_owned()))
    );

    let all_rows = prepared_all(&db, &query, ReadOpts::default());
    assert_eq!(row_ids(&all_rows), vec![todo]);
    assert_eq!(all_rows[0].cell(table, "done"), Some(Value::Bool(false)));
}

#[test]
fn local_subscription_emits_removed_row_for_fire_and_forget_delete() {
    let schema = schema();
    let owner = AuthorId::from_bytes([0x31; 16]);
    let db = open_db(0x31, owner, &schema);
    let query = Query::from("todos");
    let mut subscription = prepared_subscribe(&db, &query, ReadOpts::default()).unwrap();
    assert!(opened_rows(block_on(subscription.next_event()).unwrap()).is_empty());

    let row_id = row(0x31);
    db.insert_with_id("todos", row_id, cells("delete me", false, owner))
        .unwrap();
    let (added, updated, removed) = delta_rows(block_on(subscription.next_event()).unwrap());
    assert_eq!(row_ids(&added), vec![row_id]);
    assert!(updated.is_empty());
    assert!(removed.is_empty());

    db.delete("todos", row_id).unwrap();
    let (added, updated, removed) = delta_rows(block_on(subscription.next_event()).unwrap());
    assert!(added.is_empty());
    assert!(updated.is_empty());
    assert_eq!(
        removed
            .into_iter()
            .map(|row| row.row_uuid)
            .collect::<Vec<_>>(),
        vec![row_id]
    );
}

#[test]
fn session_scoped_subscription_emits_removed_row_for_owned_delete() {
    let schema = owner_id_public_schema();
    let author = AuthorId::from_bytes([0x32; 16]);
    let db = open_db(0x32, AuthorId::SYSTEM, &schema);
    let user_id = "local-first-user";
    db.set_identity_claims(
        author,
        BTreeMap::from([("user_id".to_owned(), Value::String(user_id.to_owned()))]),
    );
    let query = Query::from("messages");
    let prepared = prepared(&db, &query);
    let mut subscription =
        block_on(db.subscribe_for_identity(&prepared, ReadOpts::default(), author)).unwrap();
    assert!(opened_rows(block_on(subscription.next_event()).unwrap()).is_empty());

    let row_id = row(0x32);
    db.insert_with_id_for_identity(
        author,
        "messages",
        row_id,
        BTreeMap::from([
            ("body".to_owned(), Value::String("delete me".to_owned())),
            ("owner_id".to_owned(), Value::String(user_id.to_owned())),
        ]),
    )
    .unwrap();
    let (added, updated, removed) = delta_rows(block_on(subscription.next_event()).unwrap());
    assert_eq!(row_ids(&added), vec![row_id]);
    assert!(updated.is_empty());
    assert!(removed.is_empty());

    db.delete_for_identity(author, "messages", row_id).unwrap();
    let (added, updated, removed) = delta_rows(block_on(subscription.next_event()).unwrap());
    assert!(added.is_empty());
    assert!(updated.is_empty());
    assert_eq!(
        removed
            .into_iter()
            .map(|row| row.row_uuid)
            .collect::<Vec<_>>(),
        vec![row_id]
    );
}

#[test]
fn subscription_retains_a_plan_from_its_selected_authorization_mode() {
    let schema = owner_id_public_schema();
    let author = AuthorId::from_bytes([0x33; 16]);
    let db = open_db(0x33, author, &schema);
    db.set_identity_claims(
        author,
        BTreeMap::from([("user_id".to_owned(), Value::String("alice".to_owned()))]),
    );
    let prepared = prepared(
        &db,
        &Query::from("messages").filter(eq(col("owner_id"), claim("user_id"))),
    );

    let client = block_on(db.subscribe(&prepared, ReadOpts::default())).unwrap();
    assert_eq!(
        client.retained_plan_authorization_mode(),
        Some(QueryAuthorizationMode::ClientLocal)
    );

    let trusted =
        block_on(db.subscribe_for_identity(&prepared, ReadOpts::default(), author)).unwrap();
    assert_eq!(
        trusted.retained_plan_authorization_mode(),
        Some(QueryAuthorizationMode::TrustedServing)
    );
}

#[test]
fn db_close_is_idempotent() {
    let db = doctest_support::block_on(doctest_support::open_todos_db()).unwrap();
    db.insert("todos", doctest_support::todo_cells("close me", false))
        .unwrap();

    db.close().unwrap();
    db.close().unwrap();
}

#[test]
fn permission_introspection_magic_columns_fail_closed_on_prepare_query() {
    let db = doctest_support::block_on(doctest_support::open_todos_db()).unwrap();

    let query = db.table("todos").select(["$canRead"]);
    let error = expect_error(db.prepare_query(&query));
    assert_eq!(error.code, ErrorCode::Query);
    assert!(
        error.message.contains("unsupported")
            && error.message.contains("permission introspection")
            && error.message.contains("$canRead"),
        "unexpected error message: {}",
        error.message
    );

    let provenance_query = db.table("todos").select(["$createdAt", "$createdBy"]);
    db.prepare_query(&provenance_query).unwrap();
}

#[test]
fn read_opts_default_and_effective_tier_preserve_local_update_contract() {
    let opts = ReadOpts::default();
    assert_eq!(opts.tier, DurabilityTier::Local);
    assert_eq!(opts.local_updates, LocalUpdates::Immediate);
    assert_eq!(opts.propagation, Propagation::Full);

    assert_eq!(
        effective_read_tier(&ReadOpts {
            tier: DurabilityTier::None,
            local_updates: LocalUpdates::Immediate,
            propagation: Propagation::LocalOnly,
            include_deleted: false,
            ..ReadOpts::default()
        }),
        DurabilityTier::Local
    );
    assert_eq!(
        effective_read_tier(&ReadOpts {
            tier: DurabilityTier::Global,
            local_updates: LocalUpdates::Immediate,
            propagation: Propagation::LocalOnly,
            include_deleted: false,
            ..ReadOpts::default()
        }),
        DurabilityTier::Global
    );
    assert_eq!(
        effective_read_tier(&ReadOpts {
            tier: DurabilityTier::None,
            local_updates: LocalUpdates::Deferred,
            propagation: Propagation::Full,
            include_deleted: false,
            ..ReadOpts::default()
        }),
        DurabilityTier::None
    );
}

#[test]
fn single_branch_read_view_uses_query_engine_branch_source_for_one_shot_reads() {
    let db = doctest_support::block_on(doctest_support::open_todos_db()).unwrap();
    let branch = BranchId(uuid::Uuid::from_bytes([0x42; 16]));
    db.node
        .node
        .borrow_mut()
        .create_branch(branch)
        .expect("create branch");
    db.node
        .node
        .borrow_mut()
        .commit_mergeable_on_branch(
            branch,
            MergeableCommit::new("todos", row(0x42), 10)
                .cells(doctest_support::todo_cells("branch-only", false)),
        )
        .expect("commit branch row");
    let query = db.table("todos");
    let prepared_query = prepared(&db, &query);
    let opts = branch_read_opts();

    let rows = doctest_support::block_on(db.all(&prepared_query, opts.clone())).unwrap();
    assert_eq!(row_ids(&rows), vec![row(0x42)]);

    let local_subscription_opts = ReadOpts {
        propagation: Propagation::LocalOnly,
        ..opts.clone()
    };
    assert_unsupported_branch_deletion_witness(expect_error(doctest_support::block_on(
        db.subscribe(&prepared_query, local_subscription_opts),
    )));

    assert_unsupported_branch_deletion_witness(expect_error(doctest_support::block_on(
        db.subscribe(&prepared_query, opts.clone()),
    )));

    let attachment = db
        .attach_query_with_opts(&prepared_query, opts.clone())
        .unwrap();
    db.detach_query(attachment);
    let attachment = db
        .attach_query_with_opts_for_identity(&prepared_query, opts.clone(), db.identity.author)
        .unwrap();
    db.detach_query(attachment);

    let snapshot =
        doctest_support::block_on(db.all_relation_snapshot(&prepared_query, opts.clone())).unwrap();
    assert_eq!(row_ids(&snapshot.rows), vec![row(0x42)]);
}

#[test]
fn oversized_register_shape_is_rejected_at_admission() {
    let schema = schema();
    let server = open_core(0x5e, AuthorId::SYSTEM, &schema);
    let huge_table = "t".repeat(MAX_SHAPE_AST_BYTES + 1);
    let ast = ShapeAst::new(Query::from(huge_table), schema.version_id());
    let error = server
        .node()
        .borrow_mut()
        .apply_sync_message(SyncMessage::RegisterShape {
            shape_id: ShapeId(uuid::Uuid::from_bytes([0x99; 16])),
            ast,
            opts: RegisterShapeOptions::default(),
        })
        .unwrap_err();
    assert!(matches!(
        error,
        crate::node::Error::UnsupportedSyncMessage("shape AST exceeds byte limit")
    ));
}

#[test]
fn oversized_content_extent_is_rejected_at_admission() {
    let schema = schema();
    let server = open_core(0x5e, AuthorId::SYSTEM, &schema);
    let extent = crate::node::content_store::Extent {
        schema: schema.version_id(),
        table: "todos".to_owned(),
        writer: AuthorId::from_bytes([0xa1; 16]),
        row: row(0x42),
        column: "body".to_owned(),
        offset: 0,
        len: (MAX_CONTENT_EXTENT_BYTES + 1) as u64,
    };
    let error = server
        .node()
        .borrow_mut()
        .apply_sync_message(SyncMessage::ContentExtents {
            extents: vec![crate::protocol::ContentExtent {
                owner: LargeValueOwnerRef::current_row(row(0x42)),
                extent,
                bytes: vec![0_u8; MAX_CONTENT_EXTENT_BYTES + 1],
            }],
        })
        .unwrap_err();
    assert!(matches!(
        error,
        crate::node::Error::UnsupportedSyncMessage("content extent exceeds byte limit")
    ));
}

#[test]
fn branch_read_view_relation_snapshot_uses_query_engine_relation_edges() {
    let schema = relation_schema();
    let db = open_db(0xc1, AuthorId::from_bytes([0xc1; 16]), &schema);
    let branch = BranchId(uuid::Uuid::from_bytes([0x42; 16]));
    db.node
        .node
        .borrow_mut()
        .create_branch(branch)
        .expect("create branch");
    db.node
        .node
        .borrow_mut()
        .commit_mergeable_on_branch(
            branch,
            MergeableCommit::new("users", row(0xa1), 10).cells(BTreeMap::from([(
                "name".to_owned(),
                Value::String("alice".to_owned()),
            )])),
        )
        .expect("commit branch user");
    db.node
        .node
        .borrow_mut()
        .commit_mergeable_on_branch(
            branch,
            MergeableCommit::new("todos", row(0x11), 11).cells(BTreeMap::from([
                ("title".to_owned(), Value::String("branch todo".to_owned())),
                ("owner_id".to_owned(), Value::Uuid(row(0xa1).0)),
            ])),
        )
        .expect("commit branch todo");

    let query = Query::from("users").array_subquery(ArraySubquery::new(
        "todosViaOwner",
        "todos",
        "owner_id",
        "id",
    ));
    let prepared_query = prepared(&db, &query);
    let snapshot =
        doctest_support::block_on(db.all_relation_snapshot(&prepared_query, branch_read_opts()))
            .unwrap();

    assert_eq!(row_ids(&snapshot.rows), vec![row(0xa1)]);
    assert!(snapshot.edges.is_empty());
    assert_eq!(
        terminal_nested_text_values(&snapshot, row(0xa1), "todosViaOwner", "title"),
        vec!["branch todo".to_owned()]
    );
}

#[test]
fn relation_query_one_shot_hop_uses_unified_query_path() {
    let schema = relation_schema();
    let db = open_db(0xc1, AuthorId::from_bytes([0xc1; 16]), &schema);
    db.insert_with_id(
        "users",
        row(0xa1),
        BTreeMap::from([("name".to_owned(), Value::String("alice".to_owned()))]),
    )
    .unwrap();
    db.insert_with_id(
        "users",
        row(0xb1),
        BTreeMap::from([("name".to_owned(), Value::String("bob".to_owned()))]),
    )
    .unwrap();
    db.insert_with_id(
        "todos",
        row(0x11),
        BTreeMap::from([
            ("title".to_owned(), Value::String("alice todo".to_owned())),
            ("owner_id".to_owned(), Value::Uuid(row(0xa1).0)),
        ]),
    )
    .unwrap();
    db.insert_with_id(
        "todos",
        row(0x22),
        BTreeMap::from([
            ("title".to_owned(), Value::String("bob todo".to_owned())),
            ("owner_id".to_owned(), Value::Uuid(row(0xb1).0)),
        ]),
    )
    .unwrap();

    let query = RelationQuery {
        rel: RelationExpr::Project {
            input: Box::new(RelationExpr::Join {
                left: Box::new(RelationExpr::Filter {
                    input: Box::new(RelationExpr::TableScan {
                        table: "users".to_owned(),
                        alias: None,
                    }),
                    predicate: RelationPredicate::Cmp {
                        left: RelationColumnRef {
                            scope: Some("users".to_owned()),
                            column: "name".to_owned(),
                        },
                        op: RelationCmpOp::Eq,
                        right: RelationValueRef::Literal(serde_json::Value::String(
                            "alice".to_owned(),
                        )),
                    },
                }),
                right: Box::new(RelationExpr::TableScan {
                    table: "todos".to_owned(),
                    alias: Some("__hop_0".to_owned()),
                }),
                on: vec![crate::query::RelationJoinCondition {
                    left: RelationColumnRef {
                        scope: Some("users".to_owned()),
                        column: "id".to_owned(),
                    },
                    right: RelationColumnRef {
                        scope: Some("__hop_0".to_owned()),
                        column: "owner_id".to_owned(),
                    },
                }],
                join_kind: RelationJoinKind::Inner,
            }),
            columns: vec![
                crate::query::RelationProjectColumn {
                    alias: "id".to_owned(),
                    expr: RelationProjectExpr::RowId(RelationRowIdRef::Current),
                },
                crate::query::RelationProjectColumn {
                    alias: "title".to_owned(),
                    expr: RelationProjectExpr::Column(RelationColumnRef {
                        scope: Some("__hop_0".to_owned()),
                        column: "title".to_owned(),
                    }),
                },
                crate::query::RelationProjectColumn {
                    alias: "owner_id".to_owned(),
                    expr: RelationProjectExpr::Column(RelationColumnRef {
                        scope: Some("__hop_0".to_owned()),
                        column: "owner_id".to_owned(),
                    }),
                },
            ],
        },
    };

    let snapshot = block_on(db.all_relation_query(&query, ReadOpts::default())).unwrap();
    assert_eq!(row_ids(&snapshot.rows), vec![row(0x11)]);
}

#[test]
fn relation_query_one_shot_hop_accepts_runtime_uuid_literal_filter() {
    let schema = relation_schema();
    let db = open_db(0xc1, AuthorId::from_bytes([0xc1; 16]), &schema);
    db.insert_with_id(
        "users",
        row(0xa1),
        BTreeMap::from([("name".to_owned(), Value::String("alice".to_owned()))]),
    )
    .unwrap();
    db.insert_with_id(
        "users",
        row(0xb1),
        BTreeMap::from([("name".to_owned(), Value::String("bob".to_owned()))]),
    )
    .unwrap();
    db.insert_with_id(
        "todos",
        row(0x11),
        BTreeMap::from([
            ("title".to_owned(), Value::String("alice todo".to_owned())),
            ("owner_id".to_owned(), Value::Uuid(row(0xa1).0)),
        ]),
    )
    .unwrap();
    db.insert_with_id(
        "todos",
        row(0x22),
        BTreeMap::from([
            ("title".to_owned(), Value::String("bob todo".to_owned())),
            ("owner_id".to_owned(), Value::Uuid(row(0xb1).0)),
        ]),
    )
    .unwrap();

    let query = RelationQuery {
        rel: RelationExpr::Project {
            input: Box::new(RelationExpr::Join {
                left: Box::new(RelationExpr::Filter {
                    input: Box::new(RelationExpr::TableScan {
                        table: "users".to_owned(),
                        alias: None,
                    }),
                    predicate: RelationPredicate::Cmp {
                        left: RelationColumnRef {
                            scope: Some("users".to_owned()),
                            column: "id".to_owned(),
                        },
                        op: RelationCmpOp::Eq,
                        right: RelationValueRef::Literal(serde_json::json!({
                            "type": "Uuid",
                            "value": row(0xa1).0.to_string(),
                        })),
                    },
                }),
                right: Box::new(RelationExpr::TableScan {
                    table: "todos".to_owned(),
                    alias: Some("__hop_0".to_owned()),
                }),
                on: vec![crate::query::RelationJoinCondition {
                    left: RelationColumnRef {
                        scope: Some("users".to_owned()),
                        column: "id".to_owned(),
                    },
                    right: RelationColumnRef {
                        scope: Some("__hop_0".to_owned()),
                        column: "owner_id".to_owned(),
                    },
                }],
                join_kind: RelationJoinKind::Inner,
            }),
            columns: vec![
                crate::query::RelationProjectColumn {
                    alias: "id".to_owned(),
                    expr: RelationProjectExpr::Column(RelationColumnRef {
                        scope: Some("__hop_0".to_owned()),
                        column: "id".to_owned(),
                    }),
                },
                crate::query::RelationProjectColumn {
                    alias: "title".to_owned(),
                    expr: RelationProjectExpr::Column(RelationColumnRef {
                        scope: Some("__hop_0".to_owned()),
                        column: "title".to_owned(),
                    }),
                },
                crate::query::RelationProjectColumn {
                    alias: "owner_id".to_owned(),
                    expr: RelationProjectExpr::Column(RelationColumnRef {
                        scope: Some("__hop_0".to_owned()),
                        column: "owner_id".to_owned(),
                    }),
                },
            ],
        },
    };

    let snapshot = block_on(db.all_relation_query(&query, ReadOpts::default())).unwrap();
    assert_eq!(row_ids(&snapshot.rows), vec![row(0x11)]);
}

#[test]
fn relation_query_one_shot_multi_hop_scalar_fk_uses_nested_join_path() {
    let schema = relation_hop_schema();
    let db = open_db(0xc1, AuthorId::from_bytes([0xc1; 16]), &schema);
    db.insert_with_id(
        "orgs",
        row(0x01),
        BTreeMap::from([("name".to_owned(), Value::String("Org A".to_owned()))]),
    )
    .unwrap();
    db.insert_with_id(
        "orgs",
        row(0x02),
        BTreeMap::from([("name".to_owned(), Value::String("Org B".to_owned()))]),
    )
    .unwrap();
    db.insert_with_id(
        "teams",
        row(0x11),
        BTreeMap::from([
            ("name".to_owned(), Value::String("Team A".to_owned())),
            (
                "org_id".to_owned(),
                Value::Nullable(Some(Box::new(Value::Uuid(row(0x01).0)))),
            ),
        ]),
    )
    .unwrap();
    db.insert_with_id(
        "users",
        row(0x21),
        BTreeMap::from([
            ("name".to_owned(), Value::String("User A".to_owned())),
            (
                "team_id".to_owned(),
                Value::Nullable(Some(Box::new(Value::Uuid(row(0x11).0)))),
            ),
        ]),
    )
    .unwrap();

    let query = users_to_orgs_relation_query();

    let snapshot = block_on(db.all_relation_query(&query, ReadOpts::default())).unwrap();
    assert_eq!(row_ids(&snapshot.rows), vec![row(0x01)]);
}

#[test]
fn relation_query_subscription_hop_uses_unified_query_path() {
    let schema = relation_schema();
    let db = open_db(0xc1, AuthorId::from_bytes([0xc1; 16]), &schema);
    db.insert_with_id(
        "users",
        row(0xa1),
        BTreeMap::from([("name".to_owned(), Value::String("alice".to_owned()))]),
    )
    .unwrap();
    db.insert_with_id(
        "todos",
        row(0x11),
        BTreeMap::from([
            ("title".to_owned(), Value::String("alice todo".to_owned())),
            ("owner_id".to_owned(), Value::Uuid(row(0xa1).0)),
        ]),
    )
    .unwrap();

    let query = RelationQuery {
        rel: RelationExpr::Project {
            input: Box::new(RelationExpr::Join {
                left: Box::new(RelationExpr::TableScan {
                    table: "users".to_owned(),
                    alias: None,
                }),
                right: Box::new(RelationExpr::TableScan {
                    table: "todos".to_owned(),
                    alias: Some("__hop_0".to_owned()),
                }),
                on: vec![crate::query::RelationJoinCondition {
                    left: RelationColumnRef {
                        scope: Some("users".to_owned()),
                        column: "id".to_owned(),
                    },
                    right: RelationColumnRef {
                        scope: Some("__hop_0".to_owned()),
                        column: "owner_id".to_owned(),
                    },
                }],
                join_kind: RelationJoinKind::Inner,
            }),
            columns: vec![
                crate::query::RelationProjectColumn {
                    alias: "id".to_owned(),
                    expr: RelationProjectExpr::RowId(RelationRowIdRef::Current),
                },
                crate::query::RelationProjectColumn {
                    alias: "title".to_owned(),
                    expr: RelationProjectExpr::Column(RelationColumnRef {
                        scope: Some("__hop_0".to_owned()),
                        column: "title".to_owned(),
                    }),
                },
                crate::query::RelationProjectColumn {
                    alias: "owner_id".to_owned(),
                    expr: RelationProjectExpr::Column(RelationColumnRef {
                        scope: Some("__hop_0".to_owned()),
                        column: "owner_id".to_owned(),
                    }),
                },
            ],
        },
    };

    let mut stream = block_on(db.subscribe_relation_query(&query, ReadOpts::default())).unwrap();
    let opened = opened_rows(stream.try_next_event().expect("opened event"));
    assert_eq!(row_ids(&opened), vec![row(0x11)]);
}

#[test]
fn relation_query_subscription_multi_hop_scalar_fk_uses_nested_join_path() {
    let schema = relation_hop_schema();
    let db = open_db(0xc1, AuthorId::from_bytes([0xc1; 16]), &schema);
    let query = users_to_orgs_relation_query();
    let mut stream = block_on(db.subscribe_relation_query(&query, ReadOpts::default())).unwrap();
    assert!(opened_rows(stream.try_next_event().expect("opened event")).is_empty());

    db.insert_with_id(
        "orgs",
        row(0x01),
        BTreeMap::from([("name".to_owned(), Value::String("Org A".to_owned()))]),
    )
    .unwrap();
    db.insert_with_id(
        "teams",
        row(0x11),
        BTreeMap::from([
            ("name".to_owned(), Value::String("Team A".to_owned())),
            (
                "org_id".to_owned(),
                Value::Nullable(Some(Box::new(Value::Uuid(row(0x01).0)))),
            ),
        ]),
    )
    .unwrap();
    db.insert_with_id(
        "users",
        row(0x21),
        BTreeMap::from([
            ("name".to_owned(), Value::String("User A".to_owned())),
            (
                "team_id".to_owned(),
                Value::Nullable(Some(Box::new(Value::Uuid(row(0x11).0)))),
            ),
        ]),
    )
    .unwrap();

    let opened = opened_rows(stream.try_next_event().expect("opened event"));
    assert_eq!(row_ids(&opened), vec![row(0x01)]);
}

fn users_to_orgs_relation_query() -> RelationQuery {
    RelationQuery {
        rel: RelationExpr::Project {
            input: Box::new(RelationExpr::Join {
                left: Box::new(RelationExpr::Join {
                    left: Box::new(RelationExpr::TableScan {
                        table: "users".to_owned(),
                        alias: None,
                    }),
                    right: Box::new(RelationExpr::TableScan {
                        table: "teams".to_owned(),
                        alias: Some("__hop_0".to_owned()),
                    }),
                    on: vec![crate::query::RelationJoinCondition {
                        left: RelationColumnRef {
                            scope: Some("users".to_owned()),
                            column: "team_id".to_owned(),
                        },
                        right: RelationColumnRef {
                            scope: Some("__hop_0".to_owned()),
                            column: "id".to_owned(),
                        },
                    }],
                    join_kind: RelationJoinKind::Inner,
                }),
                right: Box::new(RelationExpr::TableScan {
                    table: "orgs".to_owned(),
                    alias: Some("__hop_1".to_owned()),
                }),
                on: vec![crate::query::RelationJoinCondition {
                    left: RelationColumnRef {
                        scope: Some("__hop_0".to_owned()),
                        column: "org_id".to_owned(),
                    },
                    right: RelationColumnRef {
                        scope: Some("__hop_1".to_owned()),
                        column: "id".to_owned(),
                    },
                }],
                join_kind: RelationJoinKind::Inner,
            }),
            columns: vec![
                crate::query::RelationProjectColumn {
                    alias: "id".to_owned(),
                    expr: RelationProjectExpr::Column(RelationColumnRef {
                        scope: Some("__hop_1".to_owned()),
                        column: "id".to_owned(),
                    }),
                },
                crate::query::RelationProjectColumn {
                    alias: "name".to_owned(),
                    expr: RelationProjectExpr::Column(RelationColumnRef {
                        scope: Some("__hop_1".to_owned()),
                        column: "name".to_owned(),
                    }),
                },
            ],
        },
    }
}

#[test]
fn relation_query_gather_uses_unified_reachable_lowering_for_reads_and_subscriptions() {
    // This is an integration-level facade test: the public relation-query read
    // and subscription APIs must both use the same maintained reachability
    // program for the canonical gather IR emitted by the TypeScript builder.
    let schema = JazzSchema::new([TableSchema::new(
        "teams",
        [
            ColumnSchema::new("name", ColumnType::String),
            ColumnSchema::new(
                "parent_id",
                ColumnType::Nullable(Box::new(ColumnType::Uuid)),
            ),
        ],
    )
    .with_reference("parent_id", "teams")
    .with_read_policy(Policy::public())
    .with_write_policy(Policy::public())]);
    let db = open_db(0xc1, AuthorId::from_bytes([0xc1; 16]), &schema);
    let query = teams_gather_relation_query();
    let mut stream = block_on(db.subscribe_relation_query(&query, ReadOpts::default())).unwrap();
    assert!(opened_rows(stream.try_next_event().expect("opened event")).is_empty());

    let root = row(0x01);
    let middle = row(0x02);
    let leaf = row(0x03);
    db.insert_with_id(
        "teams",
        root,
        BTreeMap::from([("name".to_owned(), Value::String("root".to_owned()))]),
    )
    .unwrap();
    db.insert_with_id(
        "teams",
        middle,
        BTreeMap::from([
            ("name".to_owned(), Value::String("middle".to_owned())),
            (
                "parent_id".to_owned(),
                Value::Nullable(Some(Box::new(Value::Uuid(root.0)))),
            ),
        ]),
    )
    .unwrap();
    db.insert_with_id(
        "teams",
        leaf,
        BTreeMap::from([
            ("name".to_owned(), Value::String("leaf".to_owned())),
            (
                "parent_id".to_owned(),
                Value::Nullable(Some(Box::new(Value::Uuid(middle.0)))),
            ),
        ]),
    )
    .unwrap();

    let changed = opened_rows(stream.try_next_event().expect("gathered rows event"));
    assert_eq!(
        row_ids(&changed).into_iter().collect::<BTreeSet<_>>(),
        BTreeSet::from([root, middle, leaf])
    );

    let snapshot = block_on(db.all_relation_query(&query, ReadOpts::default())).unwrap();
    assert_eq!(
        row_ids(&snapshot.rows).into_iter().collect::<BTreeSet<_>>(),
        BTreeSet::from([root, middle, leaf])
    );

    let filtered_query = RelationQuery {
        rel: RelationExpr::Filter {
            input: Box::new(query.rel.clone()),
            predicate: RelationPredicate::Cmp {
                left: RelationColumnRef {
                    scope: Some("teams".to_owned()),
                    column: "name".to_owned(),
                },
                op: RelationCmpOp::Ne,
                right: RelationValueRef::Literal(serde_json::Value::String("middle".to_owned())),
            },
        },
    };
    let filtered = block_on(db.all_relation_query(&filtered_query, ReadOpts::default())).unwrap();
    assert_eq!(
        row_ids(&filtered.rows).into_iter().collect::<BTreeSet<_>>(),
        BTreeSet::from([root, leaf])
    );

    let or_true = RelationQuery {
        rel: RelationExpr::Filter {
            input: Box::new(query.rel.clone()),
            predicate: RelationPredicate::Or(vec![
                RelationPredicate::True,
                RelationPredicate::False,
            ]),
        },
    };
    let unfiltered = block_on(db.all_relation_query(&or_true, ReadOpts::default())).unwrap();
    assert_eq!(
        row_ids(&unfiltered.rows)
            .into_iter()
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([root, middle, leaf])
    );

    let not_true = RelationQuery {
        rel: RelationExpr::Filter {
            input: Box::new(query.rel.clone()),
            predicate: RelationPredicate::Not(Box::new(RelationPredicate::True)),
        },
    };
    let empty = block_on(db.all_relation_query(&not_true, ReadOpts::default())).unwrap();
    assert!(empty.rows.is_empty());

    let filter_after_limit = RelationQuery {
        rel: RelationExpr::Filter {
            input: Box::new(RelationExpr::Limit {
                input: Box::new(RelationExpr::OrderBy {
                    input: Box::new(query.rel.clone()),
                    terms: vec![RelationOrderBy {
                        column: RelationColumnRef {
                            scope: Some("teams".to_owned()),
                            column: "name".to_owned(),
                        },
                        direction: OrderDirection::Asc,
                    }],
                }),
                limit: 1,
            }),
            predicate: RelationPredicate::Cmp {
                left: RelationColumnRef {
                    scope: Some("teams".to_owned()),
                    column: "name".to_owned(),
                },
                op: RelationCmpOp::Eq,
                right: RelationValueRef::Literal(serde_json::Value::String("root".to_owned())),
            },
        },
    };
    let error =
        block_on(db.all_relation_query(&filter_after_limit, ReadOpts::default())).unwrap_err();
    assert_eq!(error.code, ErrorCode::Query);
    assert!(
        error
            .message
            .contains("gather output filters cannot wrap limit or offset")
    );
}

fn teams_gather_relation_query() -> RelationQuery {
    RelationQuery {
        rel: RelationExpr::Gather {
            seed: Box::new(RelationExpr::Filter {
                input: Box::new(RelationExpr::TableScan {
                    table: "teams".to_owned(),
                    alias: None,
                }),
                predicate: RelationPredicate::Cmp {
                    left: RelationColumnRef {
                        scope: Some("teams".to_owned()),
                        column: "name".to_owned(),
                    },
                    op: RelationCmpOp::Eq,
                    right: RelationValueRef::Literal(serde_json::Value::String("leaf".to_owned())),
                },
            }),
            step: Box::new(RelationExpr::Project {
                input: Box::new(RelationExpr::Join {
                    left: Box::new(RelationExpr::Filter {
                        input: Box::new(RelationExpr::TableScan {
                            table: "teams".to_owned(),
                            alias: None,
                        }),
                        predicate: RelationPredicate::And(vec![RelationPredicate::Cmp {
                            left: RelationColumnRef {
                                scope: Some("teams".to_owned()),
                                column: "id".to_owned(),
                            },
                            op: RelationCmpOp::Eq,
                            right: RelationValueRef::RowId(RelationRowIdRef::Frontier),
                        }]),
                    }),
                    right: Box::new(RelationExpr::TableScan {
                        table: "teams".to_owned(),
                        alias: Some("__recursive_hop_0".to_owned()),
                    }),
                    on: vec![crate::query::RelationJoinCondition {
                        left: RelationColumnRef {
                            scope: Some("teams".to_owned()),
                            column: "parent_id".to_owned(),
                        },
                        right: RelationColumnRef {
                            scope: Some("__recursive_hop_0".to_owned()),
                            column: "id".to_owned(),
                        },
                    }],
                    join_kind: RelationJoinKind::Inner,
                }),
                columns: vec![crate::query::RelationProjectColumn {
                    alias: "id".to_owned(),
                    expr: RelationProjectExpr::Column(RelationColumnRef {
                        scope: Some("__recursive_hop_0".to_owned()),
                        column: "id".to_owned(),
                    }),
                }],
            }),
            frontier_key: crate::query::RelationKeyRef::RowId(RelationRowIdRef::Current),
            bound: crate::query::RecursionBound::MaxDepth(10),
            dedupe_key: vec![crate::query::RelationKeyRef::RowId(
                RelationRowIdRef::Current,
            )],
        },
    }
}

#[test]
fn relation_snapshot_reverse_array_skips_deleted_children() {
    let schema = relation_schema();
    let db = open_db(0xc1, AuthorId::from_bytes([0xc1; 16]), &schema);
    db.insert_with_id(
        "users",
        row(0xa1),
        BTreeMap::from([("name".to_owned(), Value::String("alice".to_owned()))]),
    )
    .unwrap();
    db.insert_with_id(
        "todos",
        row(0x11),
        BTreeMap::from([
            ("title".to_owned(), Value::String("deleted todo".to_owned())),
            ("owner_id".to_owned(), Value::Uuid(row(0xa1).0)),
        ]),
    )
    .unwrap();
    db.insert_with_id(
        "todos",
        row(0x22),
        BTreeMap::from([
            ("title".to_owned(), Value::String("visible todo".to_owned())),
            ("owner_id".to_owned(), Value::Uuid(row(0xa1).0)),
        ]),
    )
    .unwrap();
    db.delete("todos", row(0x11)).unwrap();

    let query = Query::from("users")
        .filter(eq(col("id"), lit(Value::Uuid(row(0xa1).0))))
        .array_subquery(ArraySubquery::new(
            "todosViaOwner",
            "todos",
            "owner_id",
            "id",
        ))
        .limit(1);
    let prepared = db.prepare_query(&query).unwrap();
    let snapshot = block_on(db.all_relation_snapshot(&prepared, ReadOpts::default())).unwrap();
    assert_eq!(row_ids(&snapshot.rows), vec![row(0xa1)]);
    assert!(snapshot.edges.is_empty());
    assert_eq!(
        terminal_nested_text_values(&snapshot, row(0xa1), "todosViaOwner", "title"),
        vec!["visible todo".to_owned()]
    );
}

#[test]
fn maintained_subscription_with_two_reference_includes_opens_with_source_coverage() {
    let schema = access_edge_include_schema();
    let client_author = AuthorId::from_bytes([0xc1; 16]);
    let server = open_core(0xee, AuthorId::SYSTEM, &schema);
    server
        .insert_with_id(
            "teams",
            row(0xa1),
            BTreeMap::from([("name".to_owned(), Value::String("resource team".to_owned()))]),
        )
        .unwrap();
    server
        .insert_with_id(
            "teams",
            row(0xb1),
            BTreeMap::from([("name".to_owned(), Value::String("member team".to_owned()))]),
        )
        .unwrap();
    server
        .insert_with_id(
            "team_access_edges",
            row(0xc1),
            BTreeMap::from([
                ("resource_id".to_owned(), Value::Uuid(row(0xa1).0)),
                ("team_id".to_owned(), Value::Uuid(row(0xb1).0)),
            ]),
        )
        .unwrap();

    let query = Query::from("team_access_edges")
        .include("resource_id")
        .include("team_id");
    let shape = query.validate(&schema).unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    let subscription = SubscriptionKey {
        shape_id: shape.shape_id(),
        binding_id: binding.binding_id(),
        read_view: RegisterShapeOptions::default().read_view_key(),
    };

    let (mut client_transport, server_transport) = duplex();
    let subscriber = server.accept_subscriber(server_transport, client_author);
    client_transport
        .send(SyncMessage::RegisterShape {
            shape_id: shape.shape_id(),
            ast: ShapeAst::from_validated(&shape),
            opts: RegisterShapeOptions::default(),
        })
        .unwrap();
    client_transport
        .send(SyncMessage::Subscribe(Subscribe {
            shape_id: shape.shape_id(),
            subscription,
            values: Vec::new(),
            known_state: None,
        }))
        .unwrap();

    subscriber.borrow_mut().tick().unwrap();
    let message = try_recv_subscriber_payload(client_transport.as_mut())
        .expect("expected include subscription view update");
    let SyncMessage::ViewUpdate {
        subscription: served,
        result_member_adds,
        ..
    } = message
    else {
        panic!("expected include subscription view update, got {message:?}");
    };
    assert_eq!(served, subscription);
    let tables = result_member_adds
        .iter()
        .filter_map(|member| member.as_real_row().map(|row| row.table.as_str()))
        .collect::<Vec<_>>();
    assert_eq!(tables, vec!["team_access_edges", "teams", "teams"]);

    client_transport
        .send(SyncMessage::Unsubscribe { subscription })
        .unwrap();
    subscriber.borrow_mut().tick().unwrap();
    client_transport
        .send(SyncMessage::Subscribe(Subscribe {
            shape_id: shape.shape_id(),
            subscription,
            values: Vec::new(),
            known_state: None,
        }))
        .unwrap();

    subscriber.borrow_mut().tick().unwrap();
    let message = try_recv_subscriber_payload(client_transport.as_mut())
        .expect("expected reopened include subscription view update");
    let SyncMessage::ViewUpdate {
        subscription: served,
        result_member_adds,
        ..
    } = message
    else {
        panic!("expected reopened include subscription view update, got {message:?}");
    };
    assert_eq!(served, subscription);
    let tables = result_member_adds
        .iter()
        .filter_map(|member| member.as_real_row().map(|row| row.table.as_str()))
        .collect::<Vec<_>>();
    assert_eq!(tables, vec!["team_access_edges", "teams", "teams"]);
}

#[test]
fn relation_snapshot_reverse_array_skips_deleted_children_with_camel_case_ref() {
    let schema = JazzSchema::new([
        TableSchema::new("users", [ColumnSchema::new("name", ColumnType::String)])
            .with_read_policy(Policy::public())
            .with_write_policy(Policy::public()),
        TableSchema::new(
            "todos",
            [
                ColumnSchema::new("title", ColumnType::String),
                ColumnSchema::new("done", ColumnType::Bool),
                ColumnSchema::new("ownerId", ColumnType::nullable(ColumnType::Uuid)),
            ],
        )
        .with_reference("ownerId", "users")
        .with_read_policy(Policy::public())
        .with_write_policy(Policy::public()),
    ]);
    let db = open_db(0xc1, AuthorId::from_bytes([0xc1; 16]), &schema);
    db.insert_with_id(
        "users",
        row(0xa1),
        BTreeMap::from([("name".to_owned(), Value::String("alice".to_owned()))]),
    )
    .unwrap();
    db.insert_with_id(
        "todos",
        row(0x11),
        BTreeMap::from([
            ("title".to_owned(), Value::String("deleted todo".to_owned())),
            ("done".to_owned(), Value::Bool(false)),
            (
                "ownerId".to_owned(),
                Value::Nullable(Some(Box::new(Value::Uuid(row(0xa1).0)))),
            ),
        ]),
    )
    .unwrap();
    db.insert_with_id(
        "todos",
        row(0x22),
        BTreeMap::from([
            ("title".to_owned(), Value::String("visible todo".to_owned())),
            ("done".to_owned(), Value::Bool(false)),
            (
                "ownerId".to_owned(),
                Value::Nullable(Some(Box::new(Value::Uuid(row(0xa1).0)))),
            ),
        ]),
    )
    .unwrap();
    let joined_before_delete = prepared_read(
        &db,
        &Query::from("users").join_via_column("todos", "ownerId", "id", []),
    );
    assert_eq!(row_ids(&joined_before_delete), vec![row(0xa1), row(0xa1)]);
    let occurrence = |joined| {
        OutputOccurrenceId::new(
            ObjectId::from_uuid(row(0xa1).0),
            [ObjectId::from_uuid(row(joined).0)],
        )
    };
    let joined_snapshot = RelationSnapshot {
        root_count: joined_before_delete.len(),
        rows: joined_before_delete.clone(),
        edges: Vec::new(),
    };
    assert!(subscription_outputs_with_occurrence_sidecar(&joined_snapshot, &[]).is_err());
    assert!(
        subscription_outputs_with_occurrence_sidecar(
            &joined_snapshot,
            &[occurrence(0x11), occurrence(0x11)],
        )
        .is_err()
    );
    assert!(
        subscription_outputs_with_occurrence_sidecar(
            &joined_snapshot,
            &[
                OutputOccurrenceId::single_source(ObjectId::from_uuid(row(0xbb).0)),
                occurrence(0x22),
            ],
        )
        .is_err()
    );
    let joined_query = Query::from("users").join_via_column("todos", "ownerId", "id", []);
    let prepared_join = prepared(&db, &joined_query);
    let mut subscription = block_on(db.subscribe(&prepared_join, ReadOpts::default())).unwrap();
    let SubscriptionEvent::Delta { added, .. } = block_on(subscription.next_event()).unwrap()
    else {
        panic!("joined subscription must start with a delta");
    };
    assert_eq!(added.len(), 2);
    let occurrence_ids = added
        .iter()
        .map(|output| output.occurrence_id.clone())
        .collect::<BTreeSet<_>>();
    assert_eq!(occurrence_ids.len(), 2);
    assert_eq!(
        added
            .iter()
            .map(|output| output.occurrence_id.clone())
            .collect::<Vec<_>>(),
        vec![occurrence(0x11), occurrence(0x22)]
    );
    assert!(
        added
            .iter()
            .all(|output| output.occurrence_id.canonical_bytes().len() == 32)
    );
    db.delete("todos", row(0x11)).unwrap();
    db.tick().unwrap();
    let SubscriptionEvent::Delta { removed, .. } = block_on(subscription.next_event()).unwrap()
    else {
        panic!("joined occurrence removal must emit a delta");
    };
    assert_eq!(removed.len(), 1);
    assert_eq!(removed[0].occurrence_id, occurrence(0x11));

    let joined = prepared_read(
        &db,
        &Query::from("users").join_via_column("todos", "ownerId", "id", []),
    );
    assert_eq!(row_ids(&joined), vec![row(0xa1)]);

    let query = Query::from("users")
        .filter(eq(col("id"), lit(Value::Uuid(row(0xa1).0))))
        .array_subquery(
            ArraySubquery::new("todosViaOwner", "todos", "ownerId", "id").select(["id"]),
        )
        .limit(1);
    let prepared = db.prepare_query(&query).unwrap();
    let snapshot = block_on(db.all_relation_snapshot(&prepared, ReadOpts::default())).unwrap();
    assert_eq!(row_ids(&snapshot.rows), vec![row(0xa1)]);
    assert!(snapshot.edges.is_empty());
    assert_eq!(
        terminal_nested_values(&snapshot, row(0xa1), "todosViaOwner", "row_uuid"),
        vec![Value::Uuid(row(0x22).0)]
    );
}

#[test]
fn relation_snapshot_reverse_array_reads_local_nullable_ref_child() {
    let schema = JazzSchema::new([
        TableSchema::new("users", [ColumnSchema::new("name", ColumnType::String)])
            .with_read_policy(Policy::public())
            .with_write_policy(Policy::public()),
        TableSchema::new(
            "todos",
            [
                ColumnSchema::new("title", ColumnType::String),
                ColumnSchema::new("ownerId", ColumnType::nullable(ColumnType::Uuid)),
            ],
        )
        .with_reference("ownerId", "users")
        .with_read_policy(Policy::public())
        .with_write_policy(Policy::public()),
    ]);
    let db = open_db(0xc1, AuthorId::from_bytes([0xc1; 16]), &schema);
    let user = db
        .insert(
            "users",
            BTreeMap::from([("name".to_owned(), Value::String("alice".to_owned()))]),
        )
        .unwrap()
        .row_uuid();
    let todo = db
        .insert(
            "todos",
            BTreeMap::from([
                ("title".to_owned(), Value::String("visible todo".to_owned())),
                (
                    "ownerId".to_owned(),
                    Value::Nullable(Some(Box::new(Value::Uuid(user.0)))),
                ),
            ]),
        )
        .unwrap()
        .row_uuid();

    let query = Query::from("users")
        .filter(eq(col("id"), lit(Value::Uuid(user.0))))
        .array_subquery(
            ArraySubquery::new("todosViaOwner", "todos", "ownerId", "id").select(["id"]),
        )
        .limit(1);
    let prepared = db.prepare_query(&query).unwrap();
    let snapshot = block_on(db.all_relation_snapshot(&prepared, ReadOpts::default())).unwrap();

    assert_eq!(row_ids(&snapshot.rows), vec![user]);
    assert!(snapshot.edges.is_empty());
    assert_eq!(
        terminal_nested_values(&snapshot, user, "todosViaOwner", "row_uuid"),
        vec![Value::Uuid(todo.0)]
    );
}

#[test]
fn relation_snapshot_reverse_array_limit_reads_local_child() {
    let schema = JazzSchema::new([
        TableSchema::new("projects", [ColumnSchema::new("name", ColumnType::String)])
            .with_read_policy(Policy::public())
            .with_write_policy(Policy::public()),
        TableSchema::new(
            "todos",
            [
                ColumnSchema::new("title", ColumnType::String),
                ColumnSchema::new("projectId", ColumnType::Uuid),
            ],
        )
        .with_reference("projectId", "projects")
        .with_read_policy(Policy::public())
        .with_write_policy(Policy::public()),
    ]);
    let db = open_db(0xc1, AuthorId::from_bytes([0xc1; 16]), &schema);
    let project = db
        .insert(
            "projects",
            BTreeMap::from([("name".to_owned(), Value::String("Announcements".to_owned()))]),
        )
        .unwrap()
        .row_uuid();
    let _todo = db
        .insert(
            "todos",
            BTreeMap::from([
                ("title".to_owned(), Value::String("visible todo".to_owned())),
                ("projectId".to_owned(), Value::Uuid(project.0)),
            ]),
        )
        .unwrap()
        .row_uuid();

    let query = Query::from("projects")
        .filter(eq(col("id"), lit(Value::Uuid(project.0))))
        .array_subquery(
            ArraySubquery::new("todosViaProject", "todos", "projectId", "id")
                .select(["title"])
                .limit(1),
        )
        .limit(1);
    let prepared = db.prepare_query(&query).unwrap();
    let snapshot = block_on(db.all_relation_snapshot(&prepared, ReadOpts::default())).unwrap();

    assert_eq!(row_ids(&snapshot.rows), vec![project]);
    assert!(snapshot.edges.is_empty());
    assert_eq!(
        terminal_nested_text_values(&snapshot, project, "todosViaProject", "title"),
        vec!["visible todo".to_owned()]
    );
}

#[test]
fn relation_snapshot_unordered_array_offset_uses_child_row_id_order() {
    let schema = relation_schema();
    let db = open_db(0xd4, AuthorId::from_bytes([0xd4; 16]), &schema);
    let parent = row(0x41);
    db.insert_with_id(
        "todos",
        parent,
        BTreeMap::from([
            ("title".to_owned(), Value::String("parent".to_owned())),
            ("owner_id".to_owned(), Value::Uuid(row(0xa1).0)),
        ]),
    )
    .unwrap();
    for id in [0xb1, 0xb2, 0xb3] {
        db.insert_with_id(
            "comments",
            row(id),
            BTreeMap::from([
                ("body".to_owned(), Value::String("tie".to_owned())),
                ("todo_id".to_owned(), Value::Uuid(parent.0)),
            ]),
        )
        .unwrap();
    }

    let query = Query::from("todos").array_subquery(
        ArraySubquery::new("comments", "comments", "todo_id", "id")
            .offset(1)
            .limit(1),
    );
    let prepared = db.prepare_query(&query).unwrap();
    let snapshot = block_on(db.all_relation_snapshot(&prepared, ReadOpts::default())).unwrap();

    assert!(snapshot.edges.is_empty());
    assert_eq!(
        terminal_nested_values(&snapshot, parent, "comments", "row_uuid"),
        vec![Value::Uuid(row(0xb2).0)]
    );
}

#[test]
fn relation_snapshot_reverse_array_projects_provenance_magic_columns() {
    let schema = JazzSchema::new([
        TableSchema::new("projects", [ColumnSchema::new("name", ColumnType::String)])
            .with_read_policy(Policy::public())
            .with_write_policy(Policy::public()),
        TableSchema::new(
            "todos",
            [
                ColumnSchema::new("title", ColumnType::String),
                ColumnSchema::new("done", ColumnType::Bool),
                ColumnSchema::new("tags", ColumnType::Array(Box::new(ColumnType::String))),
                ColumnSchema::new("projectId", ColumnType::Uuid),
                ColumnSchema::new("ownerId", ColumnType::nullable(ColumnType::Uuid)),
                ColumnSchema::new(
                    "assigneesIds",
                    ColumnType::Array(Box::new(ColumnType::Uuid)),
                ),
            ],
        )
        .with_reference("projectId", "projects")
        .with_reference("ownerId", "users")
        .with_reference("assigneesIds", "users")
        .with_read_policy(Policy::public())
        .with_write_policy(Policy::public()),
        TableSchema::new("users", [ColumnSchema::new("name", ColumnType::String)])
            .with_read_policy(Policy::public())
            .with_write_policy(Policy::public()),
    ]);
    let db = open_db(0xc1, AuthorId::from_bytes([0xc1; 16]), &schema);
    db.insert_with_id(
        "projects",
        row(0xa1),
        BTreeMap::from([("name".to_owned(), Value::String("Announcements".to_owned()))]),
    )
    .unwrap();
    db.insert_with_id(
        "todos",
        row(0x22),
        BTreeMap::from([
            ("title".to_owned(), Value::String("Write tests".to_owned())),
            ("done".to_owned(), Value::Bool(false)),
            (
                "tags".to_owned(),
                Value::Array(vec![Value::String("dev".to_owned())]),
            ),
            ("projectId".to_owned(), Value::Uuid(row(0xa1).0)),
            ("ownerId".to_owned(), Value::Nullable(None)),
            ("assigneesIds".to_owned(), Value::Array(Vec::new())),
        ]),
    )
    .unwrap();

    let query = Query::from("projects")
        .filter(eq(col("id"), lit(Value::Uuid(row(0xa1).0))))
        .array_subquery(
            ArraySubquery::new("todosViaProject", "todos", "projectId", "id")
                .select([
                    "title",
                    "done",
                    "tags",
                    "projectId",
                    "ownerId",
                    "assigneesIds",
                    "$createdAt",
                    "$updatedAt",
                ])
                .limit(1),
        )
        .limit(1);
    let prepared = db.prepare_query(&query).unwrap();
    let snapshot = block_on(db.all_relation_snapshot(&prepared, ReadOpts::default())).unwrap();
    assert_eq!(row_ids(&snapshot.rows), vec![row(0xa1)]);
    assert!(snapshot.edges.is_empty());
    assert_eq!(
        terminal_nested_values(&snapshot, row(0xa1), "todosViaProject", "row_uuid"),
        vec![Value::Uuid(row(0x22).0)]
    );
    assert!(matches!(
        terminal_nested_values(&snapshot, row(0xa1), "todosViaProject", "$createdAt").as_slice(),
        [Value::U64(_)]
    ));
    assert!(matches!(
        terminal_nested_values(&snapshot, row(0xa1), "todosViaProject", "$updatedAt").as_slice(),
        [Value::U64(_)]
    ));
}

#[test]
fn version_bearing_current_source_preserves_provenance_timestamps() {
    let db = block_on(doctest_support::open_todos_db()).unwrap();
    let id = row(0x7a);
    db.insert_with_id_at_ms(
        "todos",
        id,
        doctest_support::todo_cells("provenance", false),
        1_234,
    )
    .unwrap();
    {
        let mut node = db.node.node.borrow_mut();
        let table = node.table("todos").unwrap().clone();
        let rows = node
            .test_content_current_with_version(&table, DurabilityTier::Local)
            .unwrap();
        let created_at = rows.descriptor.field_index("created_at").unwrap();
        let record = rows
            .iter()
            .find(|(record, weight)| *weight > 0 && record.get_uuid(0).unwrap() == id.0)
            .unwrap()
            .0;
        assert_eq!(record.get_u64(created_at).unwrap(), 1_234);
    }

    let query = db
        .table("todos")
        .select(["title", "$createdAt", "$updatedAt"])
        .filter(eq(col("id"), lit(Value::Uuid(id.0))));
    let prepared = db.prepare_query(&query).unwrap();
    let rows = block_on(db.all(&prepared, ReadOpts::default())).unwrap();
    let row = rows.iter().find(|row| row.row_uuid() == id).unwrap();
    assert_eq!(row.raw_field("$createdAt"), Some(Value::U64(1_234)));
    assert_eq!(row.raw_field("$updatedAt"), Some(Value::U64(1_234)));
    assert_eq!(row.raw_field("user_done"), None);
}

#[test]
fn include_deleted_fails_closed_on_live_subscription_apis() {
    let db = doctest_support::block_on(doctest_support::open_todos_db()).unwrap();
    let query = db.table("todos");
    let prepared_query = prepared(&db, &query);
    let opts = ReadOpts {
        include_deleted: true,
        ..ReadOpts::default()
    };

    assert_unsupported_subscription_include_deleted(expect_error(doctest_support::block_on(
        db.subscribe(&prepared_query, opts.clone()),
    )));
    assert_unsupported_subscription_include_deleted(expect_error(doctest_support::block_on(
        db.subscribe_for_identity(&prepared_query, opts.clone(), db.identity.author),
    )));

    let rows = doctest_support::block_on(db.all(&prepared_query, opts)).unwrap();
    assert!(rows.is_empty());
}

#[test]
fn attached_schema_mergeable_batch_is_queryable_after_owner_commit() {
    let empty = JazzSchema::new([]);
    let refs = empty.column_families();
    let refs = refs.iter().map(String::as_str).collect::<Vec<_>>();
    let owner = block_on(Db::open_history_complete(DbConfig {
        schema: empty,
        storage: doctest_support::MemoryStorage::new(&refs),
        identity: DbIdentity {
            node: NodeUuid::from_bytes([0x91; 16]),
            author: AuthorId::SYSTEM,
        },
        id_source: Some(Box::new(SeededRowIdSource::new(91))),
        large_value_checkpoint_op_interval: crate::node::LARGE_VALUE_CHECKPOINT_OP_INTERVAL,
    }))
    .unwrap();
    let schema = JazzSchema::new([TableSchema::new(
        "todos",
        [
            ColumnSchema::new("title", ColumnType::String),
            ColumnSchema::new("done", ColumnType::Bool),
        ],
    )]);
    let view = owner.register_schema_view(schema.clone()).unwrap();
    let open = OpenBatchId::new();
    owner.begin_mergeable(open).unwrap();
    let inserted = row(0x91);
    view.mergeable_tx_ref(open)
        .insert_with_id_at_ms(
            "todos",
            inserted,
            doctest_support::todo_cells("attached", false),
            1_704_067_200_123,
        )
        .unwrap();
    owner.commit_mergeable_handle(open).unwrap();

    // Advance the owner's canonical schema after the query view was registered.
    // The historical view still calls this column `title`; resolving projection
    // against the canonical schema would silently omit it after the rename.
    let renamed_schema = JazzSchema::new([TableSchema::new(
        "todos",
        [
            ColumnSchema::new("done", ColumnType::Bool),
            ColumnSchema::new("summary", ColumnType::String),
        ],
    )]);
    let renamed = SchemaVersion::new(renamed_schema);
    owner
        .publish_schema_with_lens(
            2,
            SchemaLineagePublication::new(
                renamed.clone(),
                MigrationLens::new(
                    schema.version_id(),
                    renamed.id,
                    vec![TableLens {
                        source_table: "todos".to_owned(),
                        target_table: "todos".to_owned(),
                        ops: vec![LensOp::RenameColumn {
                            from: "title".to_owned(),
                            to: "summary".to_owned(),
                        }],
                    }],
                ),
                Vec::<String>::new(),
                Vec::<String>::new(),
            ),
        )
        .unwrap();
    owner
        .set_current_write_schema(CurrentWriteSchema {
            revision: 2,
            schema: renamed.id,
        })
        .unwrap();

    let overlay_open = OpenBatchId::new();
    owner.begin_mergeable(overlay_open).unwrap();
    let overlay_inserted = row(0x93);
    let overlay_tx = view.mergeable_tx_ref(overlay_open);
    overlay_tx
        .insert_with_id_at_ms(
            "todos",
            overlay_inserted,
            doctest_support::todo_cells("overlay", true),
            1_704_067_200_456,
        )
        .unwrap();
    let prepared = view
        .prepare_query(
            &view
                .table("todos")
                .select(["done", "title", "$createdAt", "$updatedAt"]),
        )
        .unwrap();
    let overlay_rows = overlay_tx.all_prepared(&prepared).unwrap();
    let overlay_row = overlay_rows
        .iter()
        .find(|row| row.row_uuid() == overlay_inserted)
        .expect("staged historical-view row is visible");
    assert!(
        overlay_row
            .encoded_record()
            .0
            .field_index("user_title")
            .is_some()
    );
    assert!(
        overlay_row
            .encoded_record()
            .0
            .field_index("user_done")
            .is_some()
    );
    assert_eq!(
        overlay_row.cell_at(0),
        Some(Value::String("overlay".to_owned()))
    );
    assert_eq!(overlay_row.cell_at(1), Some(Value::Bool(true)));
    let overlay_provenance = overlay_row.provenance().unwrap().unwrap();
    assert_eq!(overlay_provenance.created_at, TxTime(1_704_067_200_456));
    assert_eq!(overlay_provenance.updated_at, TxTime(1_704_067_200_456));
    owner.abandon_transaction_handle(overlay_open).unwrap();

    let rows = block_on(view.all(&prepared, ReadOpts::default())).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].row_uuid(), inserted);
    assert!(
        rows[0]
            .encoded_record()
            .0
            .field_index("user_title")
            .is_some()
    );
    assert_eq!(
        rows[0].cell_at(0),
        Some(Value::String("attached".to_owned()))
    );
}

#[test]
fn mergeable_overlay_uses_staged_provenance_and_preserves_it_at_commit() {
    let db = block_on(doctest_support::open_todos_db()).unwrap();
    let existing = row(0xa1);
    db.insert_with_id_at_ms(
        "todos",
        existing,
        doctest_support::todo_cells("existing", false),
        100,
    )
    .unwrap();
    let inserted = row(0xa2);
    let tx = db.mergeable_tx().unwrap();
    tx.insert_with_id_at_ms(
        "todos",
        inserted,
        doctest_support::todo_cells("inserted", false),
        200,
    )
    .unwrap();
    tx.update_at_ms(
        "todos",
        existing,
        BTreeMap::from([("done".to_owned(), Value::Bool(true))]),
        300,
    )
    .unwrap();
    let query = db
        .prepare_query(
            &db.table("todos")
                .select(["title", "$createdAt", "$updatedAt"]),
        )
        .unwrap();

    let overlay = tx.all_prepared(&query).unwrap();
    let repeated = tx.all_prepared(&query).unwrap();
    assert_eq!(overlay, repeated, "transaction provenance must be stable");
    let inserted_overlay = overlay
        .iter()
        .find(|row| row.row_uuid() == inserted)
        .unwrap()
        .provenance()
        .unwrap()
        .unwrap();
    assert_eq!(inserted_overlay.created_at, TxTime(200));
    assert_eq!(inserted_overlay.updated_at, TxTime(200));
    assert_eq!(inserted_overlay.created_by, db.identity.author);
    let updated_overlay = overlay
        .iter()
        .find(|row| row.row_uuid() == existing)
        .unwrap()
        .provenance()
        .unwrap()
        .unwrap();
    assert_eq!(updated_overlay.created_at, TxTime(100));
    assert_eq!(updated_overlay.updated_at, TxTime(300));
    assert_eq!(updated_overlay.updated_by, db.identity.author);

    tx.commit().unwrap();
    let committed = db.read(&query).unwrap();
    for (row_id, staged) in [(inserted, inserted_overlay), (existing, updated_overlay)] {
        let committed = committed
            .iter()
            .find(|row| row.row_uuid() == row_id)
            .unwrap()
            .provenance()
            .unwrap()
            .unwrap();
        assert_eq!(committed.created_by, staged.created_by);
        assert_eq!(committed.created_at, staged.created_at);
        assert_eq!(committed.updated_by, staged.updated_by);
        assert_eq!(committed.updated_at, staged.updated_at);
    }
}

#[test]
fn exclusive_overlay_reserves_stable_provenance_for_insert_and_update() {
    let db = block_on(doctest_support::open_todos_db()).unwrap();
    let existing = row(0xb1);
    db.insert_with_id_at_ms(
        "todos",
        existing,
        doctest_support::todo_cells("existing", false),
        100,
    )
    .unwrap();
    let inserted = row(0xb2);
    let tx = db.exclusive_tx().unwrap();
    tx.insert_with_id(
        "todos",
        inserted,
        doctest_support::todo_cells("inserted", false),
    )
    .unwrap();
    tx.update(
        "todos",
        existing,
        BTreeMap::from([("done".to_owned(), Value::Bool(true))]),
    )
    .unwrap();
    let query = db
        .prepare_query(
            &db.table("todos")
                .select(["title", "$createdAt", "$updatedAt"]),
        )
        .unwrap();
    let overlay = tx.all_prepared(&query).unwrap();
    let repeated = tx.all_prepared(&query).unwrap();
    assert_eq!(overlay, repeated, "exclusive provenance must be stable");
    let provenance = |rows: &[CurrentRow], id| {
        rows.iter()
            .find(|row| row.row_uuid() == id)
            .unwrap()
            .provenance()
            .unwrap()
            .unwrap()
    };
    let inserted_overlay = provenance(&overlay, inserted);
    let updated_overlay = provenance(&overlay, existing);
    assert_ne!(inserted_overlay.created_at, TxTime(0));
    assert_eq!(inserted_overlay.created_at, inserted_overlay.updated_at);
    assert_eq!(updated_overlay.created_at, TxTime(100));
    assert_ne!(updated_overlay.updated_at, TxTime(0));

    tx.commit().unwrap();
    let committed = db.read(&query).unwrap();
    assert_eq!(provenance(&committed, inserted), inserted_overlay);
    assert_eq!(provenance(&committed, existing), updated_overlay);
}

#[test]
fn array_subquery_live_subscription_publishes_only_terminal_root_rows() {
    let schema = relation_schema();
    let db = open_db(0xc1, AuthorId::from_bytes([0xc1; 16]), &schema);
    db.insert_with_id(
        "users",
        row(0xa1),
        BTreeMap::from([("name".to_owned(), Value::String("alice".to_owned()))]),
    )
    .unwrap();
    db.insert_with_id(
        "users",
        row(0xb1),
        BTreeMap::from([("name".to_owned(), Value::String("bob".to_owned()))]),
    )
    .unwrap();

    let query = Query::from("users")
        .filter(eq(col("id"), lit(Value::Uuid(row(0xa1).0))))
        .array_subquery(ArraySubquery::new(
            "todosViaOwner",
            "todos",
            "owner_id",
            "id",
        ));
    let prepared_query = prepared(&db, &query);
    let mut subscription = block_on(db.subscribe(&prepared_query, ReadOpts::default())).unwrap();

    let opened = block_on(subscription.next_event()).unwrap();
    let SubscriptionEvent::Delta { .. } = &opened else {
        panic!("expected terminal reset")
    };
    let snapshot = snapshot_from_event(opened);
    assert_eq!(
        terminal_nested_text_values(&snapshot, row(0xa1), "todosViaOwner", "title"),
        Vec::<String>::new(),
        "an empty nested collection is encoded in the surviving root"
    );
    assert!(snapshot.edges.is_empty());

    db.insert_with_id(
        "todos",
        row(0x11),
        BTreeMap::from([
            ("title".to_owned(), Value::String("first".to_owned())),
            ("owner_id".to_owned(), Value::Uuid(row(0xa1).0)),
        ]),
    )
    .unwrap();
    db.tick().unwrap();
    let mut child_added = block_on(subscription.next_event()).unwrap();
    while let Some(next) = subscription.try_next_event() {
        child_added = next;
    }
    let SubscriptionEvent::Delta {
        reset,
        added,
        updated,
        removed,
        terminal_operations,
        ..
    } = &child_added
    else {
        panic!("expected root replacement")
    };
    assert!(!*reset, "a child insertion must remain incremental");
    assert!(added.is_empty());
    assert!(
        updated.is_empty(),
        "a descendant patch does not replace its root"
    );
    assert!(removed.is_empty());
    assert!(
        terminal_operations
            .iter()
            .any(|operation| matches!(operation.edit, groove::ivm::TerminalEdit::Insert { .. })),
        "child insertion is delivered as a terminal path insert"
    );

    db.update(
        "todos",
        row(0x11),
        BTreeMap::from([("owner_id".to_owned(), Value::Uuid(row(0xb1).0))]),
    )
    .unwrap();
    let removed_child = block_on(subscription.next_event()).unwrap();
    assert!(matches!(
        removed_child,
        SubscriptionEvent::Delta { terminal_operations, .. }
            if terminal_operations.iter().any(|operation| matches!(operation.edit, groove::ivm::TerminalEdit::Remove { .. }))
    ));

    db.update(
        "todos",
        row(0x11),
        BTreeMap::from([("owner_id".to_owned(), Value::Uuid(row(0xa1).0))]),
    )
    .unwrap();
    let restored_child = block_on(subscription.next_event()).unwrap();
    assert!(matches!(
        restored_child,
        SubscriptionEvent::Delta { terminal_operations, .. }
            if terminal_operations.iter().any(|operation| matches!(operation.edit, groove::ivm::TerminalEdit::Insert { .. }))
    ));
}

#[test]
fn structured_subscription_splices_in_terminal_root_order_after_insert() {
    let schema = relation_schema();
    let db = open_db(0xc4, AuthorId::from_bytes([0xc4; 16]), &schema);
    db.insert_with_id(
        "users",
        row(0xa1),
        BTreeMap::from([("name".to_owned(), Value::String("zulu".to_owned()))]),
    )
    .unwrap();

    let query = Query::from("users")
        .order_by("name", OrderDirection::Asc)
        .array_subquery(ArraySubquery::new(
            "todosViaOwner",
            "todos",
            "owner_id",
            "id",
        ));
    let prepared_query = prepared(&db, &query);
    let mut subscription = block_on(db.subscribe(&prepared_query, ReadOpts::default())).unwrap();
    let initial = block_on(subscription.next_event()).unwrap();
    let snapshot = snapshot_from_event(initial);
    assert_eq!(row_ids(&snapshot.rows), vec![row(0xa1)]);

    db.insert_with_id(
        "users",
        row(0xb1),
        BTreeMap::from([("name".to_owned(), Value::String("alpha".to_owned()))]),
    )
    .unwrap();
    db.tick().unwrap();
    let reordered = block_on(subscription.next_event()).unwrap();
    let SubscriptionEvent::Delta {
        reset,
        added,
        removed,
        terminal_operations,
        ..
    } = &reordered
    else {
        panic!("expected terminal splice")
    };
    assert!(!*reset, "root reordering must remain incremental");
    assert!(removed.is_empty());
    assert!(added.is_empty());
    assert!(
        matches!(
            terminal_operations.as_slice(),
            [groove::ivm::TerminalOperation {
                path,
                edit: groove::ivm::TerminalEdit::Insert { index: 0, .. },
                ..
            }] if path.is_empty()
        ),
        "unexpected root operations: {terminal_operations:?}"
    );

    let binding_view_key = BindingViewKey::new(
        prepared_query.shape().shape_id(),
        prepared_query.binding().binding_id(),
        RegisterShapeOptions::default().read_view_key(),
    );
    db.node
        .node
        .borrow_mut()
        .inject_pending_authoritative_reset_for_test(
            binding_view_key,
            std::iter::empty(),
            GlobalSeq(0),
        );
    assert_eq!(db.refresh_subscriptions().unwrap(), 1);
    let reset = block_on(subscription.next_event()).unwrap();
    assert!(matches!(
        reset,
        SubscriptionEvent::Delta {
            reset: true,
            terminal_operations,
            ..
        } if terminal_operations.is_empty()
    ));

    db.update(
        "users",
        row(0xb1),
        BTreeMap::from([("name".to_owned(), Value::String("zzzz".to_owned()))]),
    )
    .unwrap();
    db.tick().unwrap();
    let updated = block_on(subscription.next_event()).unwrap();
    let SubscriptionEvent::Delta {
        reset,
        added,
        removed,
        terminal_operations,
        ..
    } = &updated
    else {
        panic!("expected update splice")
    };
    assert!(!*reset);
    assert!(removed.is_empty());
    assert!(added.is_empty());
    assert!(matches!(
        terminal_operations.as_slice(),
        [
            groove::ivm::TerminalOperation {
                path: remove_path,
                edit: groove::ivm::TerminalEdit::Remove { .. },
                ..
            },
            groove::ivm::TerminalOperation {
                path: insert_path,
                edit: groove::ivm::TerminalEdit::Insert { index: 1, .. },
                ..
            }
        ] if remove_path.is_empty() && insert_path.is_empty()
    ));

    db.delete("users", row(0xa1)).unwrap();
    db.tick().unwrap();
    let removed = block_on(subscription.next_event()).unwrap();
    let SubscriptionEvent::Delta { reset, .. } = &removed else {
        panic!("expected removal splice")
    };
    assert!(!*reset);
    assert!(matches!(
        removed,
        SubscriptionEvent::Delta { terminal_operations, .. }
            if matches!(
                terminal_operations.as_slice(),
                [groove::ivm::TerminalOperation {
                    path,
                    edit: groove::ivm::TerminalEdit::Remove { .. },
                    ..
                }] if path.is_empty()
            )
    ));
}

#[test]
fn flat_subscription_hydrates_in_declared_root_order() {
    let schema = relation_schema();
    let db = open_db(0xd4, AuthorId::from_bytes([0xd4; 16]), &schema);
    db.insert_with_id(
        "users",
        row(0xa1),
        BTreeMap::from([("name".to_owned(), Value::String("zulu".to_owned()))]),
    )
    .unwrap();
    db.insert_with_id(
        "users",
        row(0xb1),
        BTreeMap::from([("name".to_owned(), Value::String("alpha".to_owned()))]),
    )
    .unwrap();

    let query = Query::from("users").order_by("name", OrderDirection::Desc);
    let prepared_query = prepared(&db, &query);
    let mut subscription = block_on(db.subscribe(&prepared_query, ReadOpts::default())).unwrap();
    let initial = snapshot_from_event(block_on(subscription.next_event()).unwrap());

    assert_eq!(row_ids(&initial.rows), vec![row(0xa1), row(0xb1)]);
}

#[test]
fn flat_subscription_hydrates_in_default_row_id_order() {
    let schema = relation_schema();
    let db = open_db(0xd7, AuthorId::from_bytes([0xd7; 16]), &schema);
    for id in [0xb1, 0xa1] {
        db.insert_with_id(
            "users",
            row(id),
            BTreeMap::from([("name".to_owned(), Value::String(format!("user-{id}")))]),
        )
        .unwrap();
    }

    let prepared_query = prepared(&db, &Query::from("users"));
    let mut subscription = block_on(db.subscribe(&prepared_query, ReadOpts::default())).unwrap();
    let initial = snapshot_from_event(block_on(subscription.next_event()).unwrap());

    assert_eq!(row_ids(&initial.rows), vec![row(0xa1), row(0xb1)]);
}

#[test]
fn flat_subscription_inserts_at_declared_root_position() {
    let schema = relation_schema();
    let db = open_db(0xd5, AuthorId::from_bytes([0xd5; 16]), &schema);
    let query = Query::from("users").order_by("name", OrderDirection::Desc);
    let prepared_query = prepared(&db, &query);
    let mut subscription = block_on(db.subscribe(&prepared_query, ReadOpts::default())).unwrap();
    let _initial = block_on(subscription.next_event()).unwrap();

    for (id, name) in [(0xa1, "zulu"), (0xb1, "zzzz")] {
        db.insert_with_id(
            "users",
            row(id),
            BTreeMap::from([("name".to_owned(), Value::String(name.to_owned()))]),
        )
        .unwrap();
        db.tick().unwrap();
        let event = block_on(subscription.next_event()).unwrap();
        if id == 0xb1 {
            assert!(
                matches!(
                    &event,
                    SubscriptionEvent::Delta { terminal_operations, .. }
                        if matches!(
                            terminal_operations.as_slice(),
                            [groove::ivm::TerminalOperation {
                                path,
                                edit: groove::ivm::TerminalEdit::Insert { index: 0, .. },
                                ..
                            }] if path.is_empty()
                        )
                ),
                "unexpected flat root event: {event:?}"
            );
        }
    }

    db.update(
        "users",
        row(0xa1),
        BTreeMap::from([("name".to_owned(), Value::String("yyyy".to_owned()))]),
    )
    .unwrap();
    db.tick().unwrap();
    let event = block_on(subscription.next_event()).unwrap();
    assert!(matches!(
        event,
        SubscriptionEvent::Delta { terminal_operations, .. }
            if terminal_operations.iter().any(|operation| matches!(
                operation.edit,
                groove::ivm::TerminalEdit::Update { .. }
            ))
    ));
}

#[test]
fn flat_subscription_updates_with_nullable_sort_payload() {
    let schema = JazzSchema::new([TableSchema::new(
        "users",
        [
            ColumnSchema::new("name", ColumnType::String),
            ColumnSchema::new("rank", ColumnType::Nullable(Box::new(ColumnType::I32))),
        ],
    )
    .with_read_policy(Policy::public())
    .with_write_policy(Policy::public())]);
    let db = open_db(0xd6, AuthorId::from_bytes([0xd6; 16]), &schema);
    db.insert_with_id(
        "users",
        row(0xa1),
        BTreeMap::from([
            ("name".to_owned(), Value::String("before".to_owned())),
            (
                "rank".to_owned(),
                Value::Nullable(Some(Box::new(Value::I32(1)))),
            ),
        ]),
    )
    .unwrap();
    let query = Query::from("users").order_by("rank", OrderDirection::Asc);
    let prepared_query = prepared(&db, &query);
    let mut subscription = block_on(db.subscribe(&prepared_query, ReadOpts::default())).unwrap();
    let _initial = block_on(subscription.next_event()).unwrap();

    db.update(
        "users",
        row(0xa1),
        BTreeMap::from([("name".to_owned(), Value::String("after".to_owned()))]),
    )
    .unwrap();
    db.tick().unwrap();
    let event = block_on(subscription.next_event()).unwrap();
    assert!(matches!(
        event,
        SubscriptionEvent::Delta { terminal_operations, .. }
            if terminal_operations.iter().any(|operation| matches!(operation.edit, groove::ivm::TerminalEdit::Update { .. }))
    ));
}

#[test]
fn flat_subscription_shifts_offset_window_when_leading_row_is_deleted() {
    let schema = relation_schema();
    let db = open_db(0xd8, AuthorId::from_bytes([0xd8; 16]), &schema);
    for (id, name) in [(0xa1, "a"), (0xb1, "b"), (0xc1, "c"), (0xd1, "d")] {
        db.insert_with_id(
            "users",
            row(id),
            BTreeMap::from([("name".to_owned(), Value::String(name.to_owned()))]),
        )
        .unwrap();
    }
    let query = Query::from("users")
        .order_by("name", OrderDirection::Asc)
        .offset(1)
        .limit(2);
    let prepared_query = prepared(&db, &query);
    let mut subscription = block_on(db.subscribe(&prepared_query, ReadOpts::default())).unwrap();
    let initial = snapshot_from_event(block_on(subscription.next_event()).unwrap());
    assert_eq!(row_ids(&initial.rows), vec![row(0xb1), row(0xc1)]);

    db.delete("users", row(0xa1)).unwrap();
    db.tick().unwrap();
    let event = block_on(subscription.next_event()).unwrap();
    assert!(matches!(
        event,
        SubscriptionEvent::Delta { terminal_operations, .. }
            if terminal_operations.iter().any(|operation| matches!(operation.edit, groove::ivm::TerminalEdit::Remove { .. }))
                && terminal_operations.iter().any(|operation| matches!(operation.edit, groove::ivm::TerminalEdit::Insert { index: 1, .. }))
    ));
}

#[test]
fn array_subquery_subscription_reflects_child_mutations_and_parent_removal() {
    let schema = relation_schema();
    let db = open_db(0xc2, AuthorId::from_bytes([0xc2; 16]), &schema);
    db.insert_with_id(
        "todos",
        row(0x21),
        BTreeMap::from([
            ("title".to_owned(), Value::String("parent".to_owned())),
            ("owner_id".to_owned(), Value::Uuid(row(0xa1).0)),
        ]),
    )
    .unwrap();
    let query = Query::from("todos")
        .array_subquery(ArraySubquery::new("comments", "comments", "todo_id", "id"));
    let prepared_query = prepared(&db, &query);
    let mut subscription = block_on(db.subscribe(&prepared_query, ReadOpts::default())).unwrap();

    let snapshot = snapshot_from_event(block_on(subscription.next_event()).unwrap());
    assert_eq!(
        terminal_nested_text_values(&snapshot, row(0x21), "comments", "body"),
        Vec::<String>::new()
    );

    db.insert_with_id(
        "comments",
        row(0xc1),
        BTreeMap::from([
            ("body".to_owned(), Value::String("first".to_owned())),
            ("todo_id".to_owned(), Value::Uuid(row(0x21).0)),
        ]),
    )
    .unwrap();
    assert!(matches!(
        block_on(subscription.next_event()).unwrap(),
        SubscriptionEvent::Delta { terminal_operations, .. }
            if terminal_operations.iter().any(|operation| matches!(
                operation.edit,
                groove::ivm::TerminalEdit::Insert { .. }
            ))
    ));

    db.update(
        "comments",
        row(0xc1),
        BTreeMap::from([("body".to_owned(), Value::String("edited".to_owned()))]),
    )
    .unwrap();
    assert!(matches!(
        block_on(subscription.next_event()).unwrap(),
        SubscriptionEvent::Delta { terminal_operations, .. }
            if terminal_operations.iter().any(|operation| matches!(
                operation.edit,
                groove::ivm::TerminalEdit::Insert { .. } | groove::ivm::TerminalEdit::Update { .. }
            ))
    ));

    db.delete("comments", row(0xc1)).unwrap();
    assert!(matches!(
        block_on(subscription.next_event()).unwrap(),
        SubscriptionEvent::Delta { terminal_operations, .. }
            if terminal_operations.iter().any(|operation| matches!(
                operation.edit,
                groove::ivm::TerminalEdit::Remove { .. }
            ))
    ));

    db.insert_with_id(
        "comments",
        row(0xc2),
        BTreeMap::from([
            ("body".to_owned(), Value::String("second".to_owned())),
            ("todo_id".to_owned(), Value::Uuid(row(0x21).0)),
        ]),
    )
    .unwrap();
    assert!(matches!(
        block_on(subscription.next_event()).unwrap(),
        SubscriptionEvent::Delta { terminal_operations, .. }
            if terminal_operations.iter().any(|operation| matches!(
                operation.edit,
                groove::ivm::TerminalEdit::Insert { .. }
            ))
    ));

    db.delete("todos", row(0x21)).unwrap();
    assert!(matches!(
        block_on(subscription.next_event()).unwrap(),
        SubscriptionEvent::Delta { terminal_operations, .. }
            if terminal_operations.iter().any(|operation| operation.path.is_empty() && matches!(
                operation.edit,
                groove::ivm::TerminalEdit::Remove { .. }
            ))
    ));
}

#[test]
fn array_subquery_subscription_updates_child_order_limit_boundary() {
    let schema = relation_schema();
    let db = open_db(0xc3, AuthorId::from_bytes([0xc3; 16]), &schema);
    db.insert_with_id(
        "todos",
        row(0x31),
        BTreeMap::from([
            ("title".to_owned(), Value::String("parent".to_owned())),
            ("owner_id".to_owned(), Value::Uuid(row(0xa1).0)),
        ]),
    )
    .unwrap();
    let query = Query::from("todos").array_subquery(
        ArraySubquery::new("comments", "comments", "todo_id", "id")
            .order_by("body", OrderDirection::Asc)
            .offset(1)
            .limit(1),
    );
    let prepared_query = prepared(&db, &query);
    let mut subscription = block_on(db.subscribe(&prepared_query, ReadOpts::default())).unwrap();

    let snapshot = snapshot_from_event(block_on(subscription.next_event()).unwrap());
    assert_eq!(
        terminal_nested_text_values(&snapshot, row(0x31), "comments", "body"),
        Vec::<String>::new()
    );

    db.insert_with_id(
        "comments",
        row(0xd1),
        BTreeMap::from([
            ("body".to_owned(), Value::String("b".to_owned())),
            ("todo_id".to_owned(), Value::Uuid(row(0x31).0)),
        ]),
    )
    .unwrap();
    db.tick().unwrap();
    assert!(
        subscription.try_next_event().is_none(),
        "a child outside the actual collector window must not publish a root update"
    );
    assert_eq!(
        terminal_nested_text_values(&snapshot, row(0x31), "comments", "body"),
        Vec::<String>::new()
    );

    db.insert_with_id(
        "comments",
        row(0xd2),
        BTreeMap::from([
            ("body".to_owned(), Value::String("c".to_owned())),
            ("todo_id".to_owned(), Value::Uuid(row(0x31).0)),
        ]),
    )
    .unwrap();
    db.tick().unwrap();
    let expect_inserted_child = |event: SubscriptionEvent, expected: RowUuid| match event {
        SubscriptionEvent::Delta {
            terminal_operations,
            ..
        } => assert!(terminal_operations.iter().any(|operation| {
            matches!(
                &operation.edit,
                groove::ivm::TerminalEdit::Insert { key, .. }
                    if key.as_slice()
                        == [10]
                            .into_iter()
                            .chain(expected.0.as_bytes().iter().copied())
                            .collect::<Vec<_>>()
            )
        })),
        other => panic!("expected terminal patch event, got {other:?}"),
    };
    expect_inserted_child(block_on(subscription.next_event()).unwrap(), row(0xd2));

    db.insert_with_id(
        "comments",
        row(0xd3),
        BTreeMap::from([
            ("body".to_owned(), Value::String("a".to_owned())),
            ("todo_id".to_owned(), Value::Uuid(row(0x31).0)),
        ]),
    )
    .unwrap();
    db.tick().unwrap();
    expect_inserted_child(block_on(subscription.next_event()).unwrap(), row(0xd1));

    db.update(
        "comments",
        row(0xd3),
        BTreeMap::from([("body".to_owned(), Value::String("z".to_owned()))]),
    )
    .unwrap();
    db.tick().unwrap();
    expect_inserted_child(block_on(subscription.next_event()).unwrap(), row(0xd2));
}

#[test]
fn array_subquery_policy_oracle_filters_child_array_contents_per_identity() {
    let schema = policy_relation_schema();
    let member = AuthorId::from_bytes([0xa1; 16]);
    let other = AuthorId::from_bytes([0xb1; 16]);
    let spy = AuthorId::from_bytes([0xc1; 16]);
    let db = open_db(0xc4, AuthorId::SYSTEM, &schema);
    db.insert_with_id(
        "todos",
        row(0x41),
        BTreeMap::from([("title".to_owned(), Value::String("parent".to_owned()))]),
    )
    .unwrap();
    for (id, body, owner) in [
        (0xe1, "member-visible", member),
        (0xe2, "other-visible", other),
    ] {
        db.insert_with_id(
            "comments",
            row(id),
            BTreeMap::from([
                ("body".to_owned(), Value::String(body.to_owned())),
                ("todo_id".to_owned(), Value::Uuid(row(0x41).0)),
                ("owner".to_owned(), Value::Uuid(owner.0)),
            ]),
        )
        .unwrap();
    }
    let query = Query::from("todos")
        .array_subquery(ArraySubquery::new("comments", "comments", "todo_id", "id"));
    let prepared_query = prepared(&db, &query);

    let admin = block_on(db.all_relation_snapshot_for_identity(
        &prepared_query,
        ReadOpts::default(),
        AuthorId::SYSTEM,
    ))
    .unwrap();
    assert_eq!(
        terminal_nested_text_values(&admin, row(0x41), "comments", "body"),
        vec!["member-visible".to_owned(), "other-visible".to_owned()]
    );

    let member_snapshot = block_on(db.all_relation_snapshot_for_identity(
        &prepared_query,
        ReadOpts::default(),
        member,
    ))
    .unwrap();
    assert_eq!(
        terminal_nested_text_values(&member_snapshot, row(0x41), "comments", "body"),
        vec!["member-visible".to_owned()]
    );

    let spy_snapshot =
        block_on(db.all_relation_snapshot_for_identity(&prepared_query, ReadOpts::default(), spy))
            .unwrap();
    assert_eq!(
        terminal_nested_text_values(&spy_snapshot, row(0x41), "comments", "body"),
        Vec::<String>::new()
    );
}

#[test]
fn array_subquery_one_shot_and_maintained_subscription_are_equivalent() {
    let schema = relation_schema();
    let db = open_db(0xc5, AuthorId::from_bytes([0xc5; 16]), &schema);
    db.insert_with_id(
        "todos",
        row(0x51),
        BTreeMap::from([
            ("title".to_owned(), Value::String("parent".to_owned())),
            ("owner_id".to_owned(), Value::Uuid(row(0xa1).0)),
        ]),
    )
    .unwrap();
    for (id, body) in [(0xf1, "first"), (0xf2, "second")] {
        db.insert_with_id(
            "comments",
            row(id),
            BTreeMap::from([
                ("body".to_owned(), Value::String(body.to_owned())),
                ("todo_id".to_owned(), Value::Uuid(row(0x51).0)),
            ]),
        )
        .unwrap();
    }
    let query = Query::from("todos").array_subquery(
        ArraySubquery::new("comments", "comments", "todo_id", "id")
            .order_by("body", OrderDirection::Asc),
    );
    let prepared_query = prepared(&db, &query);
    let one_shot =
        block_on(db.all_relation_snapshot(&prepared_query, ReadOpts::default())).unwrap();
    let mut subscription = block_on(db.subscribe(&prepared_query, ReadOpts::default())).unwrap();
    let maintained = snapshot_from_event(block_on(subscription.next_event()).unwrap());

    assert_eq!(
        terminal_nested_text_values(&maintained, row(0x51), "comments", "body"),
        terminal_nested_text_values(&one_shot, row(0x51), "comments", "body")
    );
}

#[test]
fn array_subquery_subscription_projects_late_root_and_existing_forward_target() {
    let schema = relation_schema();
    let db = open_db(0xc7, AuthorId::from_bytes([0xc7; 16]), &schema);
    db.insert_with_id(
        "users",
        row(0xa1),
        BTreeMap::from([("name".to_owned(), Value::String("owner".to_owned()))]),
    )
    .unwrap();
    let query = Query::from("todos")
        .select(["title"])
        .array_subquery(ArraySubquery::new("owner", "users", "id", "owner_id").select(["name"]));
    let prepared_query = prepared(&db, &query);
    let mut subscription = block_on(db.subscribe(&prepared_query, ReadOpts::default())).unwrap();
    let opened = snapshot_from_event(block_on(subscription.next_event()).unwrap());
    assert!(opened.rows.is_empty());

    db.insert_with_id(
        "todos",
        row(0x52),
        BTreeMap::from([
            ("title".to_owned(), Value::String("late root".to_owned())),
            ("owner_id".to_owned(), Value::Uuid(row(0xa1).0)),
        ]),
    )
    .unwrap();
    assert!(matches!(
        block_on(subscription.next_event()).unwrap(),
        SubscriptionEvent::Delta { terminal_operations, .. }
            if terminal_operations.iter().any(|operation| operation.path.is_empty() && matches!(
                operation.edit,
                groove::ivm::TerminalEdit::Insert { index: 0, .. }
            ))
    ));
}

#[test]
fn array_subquery_subscription_projects_late_camel_case_root_and_existing_forward_target() {
    let schema = issue_schema();
    let db = open_db(0xc8, AuthorId::from_bytes([0xc8; 16]), &schema);
    db.insert_with_id(
        "projects",
        row(0xa2),
        BTreeMap::from([("name".to_owned(), Value::String("project".to_owned()))]),
    )
    .unwrap();
    let query = Query::from("issues").select(["title"]).array_subquery(
        ArraySubquery::new("project", "projects", "id", "project").select(["name"]),
    );
    let prepared_query = prepared(&db, &query);
    let mut subscription = block_on(db.subscribe(&prepared_query, ReadOpts::default())).unwrap();
    let opened = snapshot_from_event(block_on(subscription.next_event()).unwrap());
    assert!(opened.rows.is_empty());

    db.insert_with_id(
        "issues",
        row(0x53),
        issue_cells(
            "late issue",
            "open",
            AuthorId::from_bytes([0xa8; 16]),
            row(0xa2),
            1,
            &[],
            None,
        ),
    )
    .unwrap();
    assert!(matches!(
        block_on(subscription.next_event()).unwrap(),
        SubscriptionEvent::Delta { terminal_operations, .. }
            if terminal_operations.iter().any(|operation| operation.path.is_empty() && matches!(
                operation.edit,
                groove::ivm::TerminalEdit::Insert { index: 0, .. }
            ))
    ));
}

#[test]
fn array_subquery_remote_subscription_hydrates_edge_referenced_child_rows() {
    let schema = relation_schema();
    let server = open_core(0x5e, AuthorId::SYSTEM, &schema);
    let client_author = AuthorId::from_bytes([0xc6; 16]);
    let client = open_db(0xc6, client_author, &schema);
    let (client_transport, server_transport) = byte_duplex();
    let _upstream = client.connect_upstream(client_transport);
    let _subscriber = server.accept_subscriber(server_transport, client_author);

    let query = Query::from("users").array_subquery(ArraySubquery::new(
        "todosViaOwner",
        "todos",
        "owner_id",
        "id",
    ));
    let mut subscription = prepared_subscribe(&client, &query, global_subscribe_opts()).unwrap();
    let opened = snapshot_from_event(block_on(subscription.next_event()).unwrap());
    assert!(opened.rows.is_empty());

    server
        .insert_with_id(
            "users",
            row(0xa6),
            BTreeMap::from([("name".to_owned(), Value::String("remote user".to_owned()))]),
        )
        .unwrap();
    server
        .insert_with_id(
            "todos",
            row(0x66),
            BTreeMap::from([
                ("title".to_owned(), Value::String("remote child".to_owned())),
                ("owner_id".to_owned(), Value::Uuid(row(0xa6).0)),
            ]),
        )
        .unwrap();

    let mut delivered = None;
    for _ in 0..20 {
        client.tick().unwrap();
        server.server.tick().unwrap();
        client.tick().unwrap();
        if let Some(event) = subscription.try_next_event() {
            let snapshot = snapshot_from_event(event);
            if terminal_nested_text_values(&snapshot, row(0xa6), "todosViaOwner", "title")
                == vec!["remote child".to_owned()]
            {
                delivered = Some(snapshot);
                break;
            }
        }
    }
    assert!(
        delivered.is_some(),
        "remote maintained array subscription must deliver the Groove terminal parent"
    );

    server
        .insert_with_id(
            "todos",
            row(0x67),
            BTreeMap::from([
                ("title".to_owned(), Value::String("second child".to_owned())),
                ("owner_id".to_owned(), Value::Uuid(row(0xa6).0)),
            ]),
        )
        .unwrap();
    let mut delivered_patch = None;
    for _ in 0..20 {
        client.tick().unwrap();
        server.server.tick().unwrap();
        client.tick().unwrap();
        if let Some(event @ SubscriptionEvent::Delta { .. }) = subscription.try_next_event() {
            if matches!(
                &event,
                SubscriptionEvent::Delta {
                    reset: false,
                    terminal_operations,
                    added,
                    updated,
                    removed,
                    ..
                } if added.is_empty()
                    && updated.is_empty()
                    && removed.is_empty()
                    && terminal_operations.iter().any(|operation| {
                        !operation.path.is_empty()
                            && matches!(operation.edit, groove::ivm::TerminalEdit::Insert { .. })
                    })
            ) {
                delivered_patch = Some(event);
                break;
            }
        }
    }
    assert!(
        delivered_patch.is_some(),
        "framed peer delivery must preserve a generic terminal patch without row replacement"
    );
}

#[test]
fn client_initial_sync_flush_cadence_preserves_public_snapshot_delivery() {
    let schema = schema();
    let server = open_core(0xd4, AuthorId::SYSTEM, &schema);
    for ordinal in 0..3_u8 {
        server
            .insert_with_id(
                "todos",
                row(0xd0 + ordinal),
                BTreeMap::from([
                    (
                        "title".to_owned(),
                        Value::String(format!("server {ordinal}")),
                    ),
                    ("done".to_owned(), Value::Bool(false)),
                ]),
            )
            .unwrap();
    }

    let client_author = AuthorId::from_bytes([0xd5; 16]);
    let client = open_db(0xd5, client_author, &schema);
    client
        .set_initial_sync_flush_cadence(InitialSyncFlushCadence::every(
            NonZeroUsize::new(2).unwrap(),
        ))
        .unwrap();
    let (client_transport, server_transport) = duplex();
    let _upstream = client.connect_upstream(client_transport);
    let _subscriber = server.accept_subscriber(server_transport, client_author);
    let query = client.table("todos");
    let mut subscription = prepared_subscribe(&client, &query, global_subscribe_opts()).unwrap();
    let _ = block_on(subscription.next_event()).unwrap();

    for _ in 0..20 {
        client.tick().unwrap();
        server.server.tick().unwrap();
        client.tick().unwrap();
        if let Some(event) = subscription.try_next_event()
            && opened_rows(event).len() == 3
        {
            return;
        }
    }
    panic!("client configured with a cadence must receive the initial snapshot");
}

#[test]
fn edge_read_opts_and_wait_honor_edge_durability() {
    let db = doctest_support::block_on(doctest_support::open_todos_db()).unwrap();
    let write = db
        .insert("todos", doctest_support::todo_cells("edge observed", false))
        .unwrap();
    let query = db.table("todos");
    let prepared_query = prepared(&db, &query);

    assert_eq!(
        effective_read_tier(&ReadOpts {
            tier: DurabilityTier::Edge,
            local_updates: LocalUpdates::Immediate,
            propagation: Propagation::LocalOnly,
            include_deleted: false,
            ..ReadOpts::default()
        }),
        DurabilityTier::Edge
    );
    assert!(
        doctest_support::block_on(db.all(
            &prepared_query,
            ReadOpts {
                tier: DurabilityTier::Edge,
                local_updates: LocalUpdates::Immediate,
                propagation: Propagation::LocalOnly,
                include_deleted: false,
                ..ReadOpts::default()
            },
        ))
        .unwrap()
        .is_empty()
    );
    let not_observed = doctest_support::block_on(write.wait(DurabilityTier::Edge)).unwrap_err();
    assert_eq!(not_observed.code, ErrorCode::NotObserved);

    // E1: edge-accept produced directly; E2 wires the acceptance path.
    db.node
        .node
        .borrow_mut()
        .apply_fate_update(
            write.mergeable_tx_id(),
            Fate::Accepted,
            None,
            Some(DurabilityTier::Edge),
        )
        .unwrap();

    assert_eq!(
        doctest_support::block_on(write.wait(DurabilityTier::Edge)).unwrap(),
        write.mergeable_tx_id()
    );
    assert_eq!(
        row_ids(
            &doctest_support::block_on(db.all(
                &prepared_query,
                ReadOpts {
                    tier: DurabilityTier::Edge,
                    local_updates: LocalUpdates::Immediate,
                    propagation: Propagation::LocalOnly,
                    include_deleted: false,
                    ..ReadOpts::default()
                },
            ))
            .unwrap()
        ),
        vec![write.row_uuid()]
    );
}

#[test]
fn upsert_merges_existing_rows_but_writes_absent_rows_directly() {
    let db = doctest_support::block_on(doctest_support::open_todos_db()).unwrap();
    let table = &doctest_support::schema().tables[0];
    let existing = row(1);
    let absent = row(2);

    db.upsert(
        "todos",
        existing,
        doctest_support::todo_cells("draft", false),
    )
    .unwrap();
    db.upsert(
        "todos",
        existing,
        BTreeMap::from([("title".to_owned(), Value::String("renamed".to_owned()))]),
    )
    .unwrap();
    db.upsert(
        "todos",
        absent,
        BTreeMap::from([("title".to_owned(), Value::String("created".to_owned()))]),
    )
    .unwrap();

    let rows = prepared_read(&db, &db.table("todos"))
        .into_iter()
        .map(|row| (row.row_uuid(), row))
        .collect::<BTreeMap<_, _>>();
    assert_eq!(
        rows.get(&existing).unwrap().cell(table, "title"),
        Some(Value::String("renamed".to_owned()))
    );
    assert_eq!(
        rows.get(&existing).unwrap().cell(table, "done"),
        Some(Value::Bool(false))
    );
    assert_eq!(
        rows.get(&absent).unwrap().cell(table, "title"),
        Some(Value::String("created".to_owned()))
    );
    assert_eq!(rows.get(&absent).unwrap().cell(table, "done"), None);
}

#[test]
fn mergeable_tx_commits_multiple_writes_under_one_tx_id() {
    let db = doctest_support::block_on(doctest_support::open_todos_db()).unwrap();
    let table = &doctest_support::schema().tables[0];
    let row_one = row(1);
    let row_two = row(2);
    let tx = db.mergeable_tx().unwrap();

    tx.insert_with_id("todos", row_one, doctest_support::todo_cells("one", false))
        .unwrap();
    tx.insert_with_id("todos", row_two, doctest_support::todo_cells("two", true))
        .unwrap();
    let tx_id = tx.commit().unwrap();

    let rows = prepared_read(&db, &db.table("todos"))
        .into_iter()
        .map(|row| (row.row_uuid(), row))
        .collect::<BTreeMap<_, _>>();
    assert_eq!(
        rows.get(&row_one).unwrap().cell(table, "title"),
        Some(Value::String("one".to_owned()))
    );
    assert_eq!(
        rows.get(&row_two).unwrap().cell(table, "title"),
        Some(Value::String("two".to_owned()))
    );
    let unit = db.node.node.borrow_mut().commit_unit_for(tx_id).unwrap();
    let SyncMessage::CommitUnit { tx, versions } = unit else {
        panic!("expected commit unit");
    };
    assert_eq!(tx.tx_id, tx_id);
    assert_eq!(tx.n_total_writes, 2);
    assert_eq!(versions.len(), 2);
}

#[test]
fn mergeable_tx_coalesces_insert_then_update_for_same_row() {
    let db = doctest_support::block_on(doctest_support::open_todos_db()).unwrap();
    let table = &doctest_support::schema().tables[0];
    let row = row(1);
    let tx = db.mergeable_tx().unwrap();

    tx.insert_with_id("todos", row, doctest_support::todo_cells("draft", false))
        .unwrap();
    tx.update(
        "todos",
        row,
        BTreeMap::from([("done".to_owned(), Value::Bool(true))]),
    )
    .unwrap();
    let tx_id = tx.commit().unwrap();

    let row_after = prepared_one(&db, &db.table("todos")).unwrap();
    assert_eq!(row_after.row_uuid(), row);
    assert_eq!(
        row_after.cell(table, "title"),
        Some(Value::String("draft".to_owned()))
    );
    assert_eq!(row_after.cell(table, "done"), Some(Value::Bool(true)));

    let unit = db.node.node.borrow_mut().commit_unit_for(tx_id).unwrap();
    let SyncMessage::CommitUnit { tx, versions } = unit else {
        panic!("expected commit unit");
    };
    assert_eq!(tx.tx_id, tx_id);
    assert_eq!(tx.n_total_writes, 1);
    assert_eq!(versions.len(), 1);
}

#[test]
fn mergeable_tx_coalesces_restore_then_update_for_same_row() {
    let db = doctest_support::block_on(doctest_support::open_todos_db()).unwrap();
    let table = &doctest_support::schema().tables[0];
    let row = row(1);

    db.insert_with_id("todos", row, doctest_support::todo_cells("archived", false))
        .unwrap();
    db.delete("todos", row).unwrap();
    assert!(prepared_read(&db, &db.table("todos")).is_empty());

    let tx = db.mergeable_tx().unwrap();
    tx.restore("todos", row, doctest_support::todo_cells("restored", false))
        .unwrap();
    tx.update(
        "todos",
        row,
        BTreeMap::from([("done".to_owned(), Value::Bool(true))]),
    )
    .unwrap();
    let tx_id = tx.commit().unwrap();

    let row_after = prepared_one(&db, &db.table("todos")).unwrap();
    assert_eq!(row_after.row_uuid(), row);
    assert_eq!(
        row_after.cell(table, "title"),
        Some(Value::String("restored".to_owned()))
    );
    assert_eq!(row_after.cell(table, "done"), Some(Value::Bool(true)));

    let unit = db.node.node.borrow_mut().commit_unit_for(tx_id).unwrap();
    let SyncMessage::CommitUnit { tx, versions } = unit else {
        panic!("expected commit unit");
    };
    assert_eq!(tx.tx_id, tx_id);
    assert_eq!(tx.n_total_writes, 2);
    assert_eq!(versions.len(), 2);
    assert_eq!(
        versions
            .iter()
            .filter(|version| version.deletion().is_none())
            .count(),
        1
    );
    assert_eq!(
        versions
            .iter()
            .filter(|version| version.deletion() == Some(DeletionEvent::Restored))
            .count(),
        1
    );
}

#[test]
fn mergeable_tx_coalesces_repeated_same_row_updates() {
    let db = doctest_support::block_on(doctest_support::open_todos_db()).unwrap();
    let table = &doctest_support::schema().tables[0];
    let row = row(1);
    let tx = db.mergeable_tx().unwrap();

    tx.insert_with_id("todos", row, doctest_support::todo_cells("first", false))
        .unwrap();
    tx.update(
        "todos",
        row,
        BTreeMap::from([("title".to_owned(), Value::String("second".to_owned()))]),
    )
    .unwrap();
    tx.update(
        "todos",
        row,
        BTreeMap::from([("done".to_owned(), Value::Bool(true))]),
    )
    .unwrap();
    let tx_id = tx.commit().unwrap();

    let row_after = prepared_one(&db, &db.table("todos")).unwrap();
    assert_eq!(row_after.row_uuid(), row);
    assert_eq!(
        row_after.cell(table, "title"),
        Some(Value::String("second".to_owned()))
    );
    assert_eq!(row_after.cell(table, "done"), Some(Value::Bool(true)));

    let unit = db.node.node.borrow_mut().commit_unit_for(tx_id).unwrap();
    let SyncMessage::CommitUnit { tx, versions } = unit else {
        panic!("expected commit unit");
    };
    assert_eq!(tx.tx_id, tx_id);
    assert_eq!(tx.n_total_writes, 1);
    assert_eq!(versions.len(), 1);
}

#[test]
fn mergeable_tx_coalesces_update_then_delete_for_same_row() {
    let db = doctest_support::block_on(doctest_support::open_todos_db()).unwrap();
    let row = row(1);

    db.insert_with_id("todos", row, doctest_support::todo_cells("base", false))
        .unwrap();
    let tx = db.mergeable_tx().unwrap();
    tx.update(
        "todos",
        row,
        BTreeMap::from([("title".to_owned(), Value::String("ignored".to_owned()))]),
    )
    .unwrap();
    tx.delete("todos", row).unwrap();
    let tx_id = tx.commit().unwrap();

    assert!(prepared_read(&db, &db.table("todos")).is_empty());
    let unit = db.node.node.borrow_mut().commit_unit_for(tx_id).unwrap();
    let SyncMessage::CommitUnit { tx, versions } = unit else {
        panic!("expected commit unit");
    };
    assert_eq!(tx.tx_id, tx_id);
    assert_eq!(tx.n_total_writes, 1);
    assert_eq!(versions.len(), 1);
}

#[test]
fn mergeable_tx_and_ref_have_identical_restore_and_reinsert_results() {
    let builder = doctest_support::block_on(doctest_support::open_todos_db()).unwrap();
    let handle = doctest_support::block_on(doctest_support::open_todos_db()).unwrap();
    let table = &doctest_support::schema().tables[0];
    let restored = row(1);
    let reinserted = row(2);

    for db in [&builder, &handle] {
        db.insert_with_id(
            "todos",
            restored,
            doctest_support::todo_cells("archived", false),
        )
        .unwrap();
        db.delete("todos", restored).unwrap();
        db.insert_with_id(
            "todos",
            reinserted,
            doctest_support::todo_cells("original", false),
        )
        .unwrap();
    }

    let builder_tx = builder.mergeable_tx().unwrap();
    builder_tx
        .restore(
            "todos",
            restored,
            doctest_support::todo_cells("restored", false),
        )
        .unwrap();
    builder_tx
        .update(
            "todos",
            restored,
            BTreeMap::from([("done".to_owned(), Value::Bool(true))]),
        )
        .unwrap();
    builder_tx.delete("todos", reinserted).unwrap();
    builder_tx
        .insert_with_id(
            "todos",
            reinserted,
            doctest_support::todo_cells("reinserted", true),
        )
        .unwrap();
    builder_tx.commit().unwrap();

    let open_tx = OpenBatchId::new();
    handle.begin_mergeable(open_tx).unwrap();
    {
        let tx = handle.mergeable_tx_ref(open_tx);
        tx.restore(
            "todos",
            restored,
            doctest_support::todo_cells("restored", false),
        )
        .unwrap();
        tx.update(
            "todos",
            restored,
            BTreeMap::from([("done".to_owned(), Value::Bool(true))]),
        )
        .unwrap();
        tx.delete("todos", reinserted).unwrap();
        tx.insert_with_id(
            "todos",
            reinserted,
            doctest_support::todo_cells("reinserted", true),
        )
        .unwrap();
    }
    handle.commit_mergeable_handle(open_tx).unwrap();

    let read_state = |db: &Db<_>| {
        let query = db.prepare_query(&db.table("todos")).unwrap();
        doctest_support::block_on(db.all(
            &query,
            ReadOpts {
                include_deleted: true,
                ..ReadOpts::default()
            },
        ))
        .unwrap()
        .into_iter()
        .map(|row| {
            (
                row.row_uuid(),
                (
                    row.is_deleted(),
                    row.cell(table, "title"),
                    row.cell(table, "done"),
                ),
            )
        })
        .collect::<BTreeMap<_, _>>()
    };

    let builder_state = read_state(&builder);
    let handle_state = read_state(&handle);
    assert_eq!(builder_state, handle_state);
    assert_eq!(
        builder_state.get(&restored),
        Some(&(
            false,
            Some(Value::String("restored".to_owned())),
            Some(Value::Bool(true)),
        ))
    );
    assert_eq!(
        builder_state.get(&reinserted),
        Some(&(
            true,
            Some(Value::String("reinserted".to_owned())),
            Some(Value::Bool(true)),
        ))
    );
}

#[test]
fn mergeable_tx_read_observes_its_staged_restore() {
    let db = doctest_support::block_on(doctest_support::open_todos_db()).unwrap();
    let row = row(1);

    db.insert_with_id("todos", row, doctest_support::todo_cells("archived", false))
        .unwrap();
    db.delete("todos", row).unwrap();

    let tx = db.mergeable_tx().unwrap();
    tx.restore("todos", row, doctest_support::todo_cells("restored", false))
        .unwrap();
    tx.update(
        "todos",
        row,
        BTreeMap::from([("done".to_owned(), Value::Bool(true))]),
    )
    .unwrap();

    assert_eq!(
        tx.read("todos", row).unwrap(),
        Some(doctest_support::todo_cells("restored", true))
    );
}

#[test]
fn exclusive_tx_ref_survives_handle_reconstruction_until_explicit_commit() {
    let db = doctest_support::block_on(doctest_support::open_todos_db()).unwrap();
    let table = &doctest_support::schema().tables[0];
    let row = row(1);

    db.insert_with_id("todos", row, doctest_support::todo_cells("base", false))
        .unwrap();

    let open_tx = OpenBatchId::new();
    db.begin_exclusive(open_tx).unwrap();
    {
        let tx = db.exclusive_tx_ref(open_tx);
        assert_eq!(
            tx.read("todos", row).unwrap(),
            Some(doctest_support::todo_cells("base", false))
        );
        tx.update(
            "todos",
            row,
            BTreeMap::from([("done".to_owned(), Value::Bool(true))]),
        )
        .unwrap();
    }
    db.commit_exclusive_handle(open_tx).unwrap();

    let current = prepared_one(&db, &db.table("todos")).unwrap();
    assert_eq!(
        current.cell(table, "title"),
        Some(Value::String("base".to_owned()))
    );
    assert_eq!(current.cell(table, "done"), Some(Value::Bool(true)));
}

#[test]
fn exclusive_tx_rejects_conflicting_concurrent_update() {
    let schema = schema();
    let owner = AuthorId::from_bytes([0xa1; 16]);
    let core = open_core(0x5e, AuthorId::SYSTEM, &schema);
    let table = &schema.tables[0];
    let row = row(1);

    core.insert_with_id("todos", row, cells("base", false, owner))
        .unwrap();
    let first = core.exclusive_tx().unwrap();
    let second = core.exclusive_tx().unwrap();
    assert_eq!(
        second.read("todos", row).unwrap().unwrap().get("title"),
        Some(&Value::String("base".to_owned()))
    );

    first
        .insert_with_id("todos", row, cells("first", false, owner))
        .unwrap();
    first.commit().unwrap();
    second
        .update(
            "todos",
            row,
            BTreeMap::from([("title".to_owned(), Value::String("second".to_owned()))]),
        )
        .unwrap();

    let err = second.commit().unwrap_err();

    assert_eq!(err.code, ErrorCode::WriteRejected);
    assert!(err.message.contains("ExclusiveConflict"));
    assert_eq!(
        core.one(&core.table("todos"))
            .unwrap()
            .unwrap()
            .cell(table, "title"),
        Some(Value::String("first".to_owned()))
    );
}

#[test]
fn exclusive_tx_blind_writes_are_first_committer_wins() {
    // Two concurrent exclusive transactions overwrite the same existing row
    // WITHOUT reading it. With no read sets, only per-write first-committer-wins
    // (INV-TX-20) can catch the conflict — this is the exact case the earlier
    // broken validator let through (it short-circuited to "ok" on empty reads).
    let schema = schema();
    let owner = AuthorId::from_bytes([0xa1; 16]);
    let core = open_core(0x5e, AuthorId::SYSTEM, &schema);
    let table = &schema.tables[0];
    let row = row(1);

    core.insert_with_id("todos", row, cells("base", false, owner))
        .unwrap();

    let first = core.exclusive_tx().unwrap();
    let second = core.exclusive_tx().unwrap();
    first
        .insert_with_id("todos", row, cells("first", false, owner))
        .unwrap();
    second
        .insert_with_id("todos", row, cells("second", false, owner))
        .unwrap();

    first.commit().unwrap();
    let err = second.commit().unwrap_err();
    assert_eq!(err.code, ErrorCode::TransactionConflict);
    assert!(err.message.contains("visible parent changed"));
    assert_eq!(
        core.one(&core.table("todos"))
            .unwrap()
            .unwrap()
            .cell(table, "title"),
        Some(Value::String("first".to_owned()))
    );
}

#[test]
fn db_facade_mutation_lifecycle_writes_reads_deletes_and_restores() {
    let db = doctest_support::block_on(doctest_support::open_todos_db()).unwrap();
    let query = db.table("todos");
    let table = &doctest_support::schema().tables[0];

    let write = db
        .insert("todos", doctest_support::todo_cells("draft todo", false))
        .unwrap();
    let todo = write.row_uuid();
    doctest_support::block_on(write.wait(DurabilityTier::Local)).unwrap();

    let rows = prepared_read(&db, &query);
    assert_eq!(row_ids(&rows), vec![todo]);
    assert_eq!(
        rows[0].cell(table, "title"),
        Some(Value::String("draft todo".to_owned()))
    );
    assert_eq!(rows[0].cell(table, "done"), Some(Value::Bool(false)));

    let write = db
        .update(
            "todos",
            todo,
            BTreeMap::from([("done".to_owned(), Value::Bool(true))]),
        )
        .unwrap();
    doctest_support::block_on(write.wait(DurabilityTier::Local)).unwrap();

    let rows = prepared_read(&db, &query);
    assert_eq!(row_ids(&rows), vec![todo]);
    assert_eq!(
        rows[0].cell(table, "title"),
        Some(Value::String("draft todo".to_owned()))
    );
    assert_eq!(rows[0].cell(table, "done"), Some(Value::Bool(true)));

    let write = db.delete("todos", todo).unwrap();
    doctest_support::block_on(write.wait(DurabilityTier::Local)).unwrap();
    assert!(prepared_read(&db, &query).is_empty());

    let write = db
        .restore(
            "todos",
            todo,
            doctest_support::todo_cells("restored todo", true),
        )
        .unwrap();
    doctest_support::block_on(write.wait(DurabilityTier::Local)).unwrap();

    let rows = prepared_read(&db, &query);
    assert_eq!(row_ids(&rows), vec![todo]);
    assert_eq!(
        rows[0].cell(table, "title"),
        Some(Value::String("restored todo".to_owned()))
    );
    assert_eq!(rows[0].cell(table, "done"), Some(Value::Bool(true)));
}

#[test]
fn db_facade_subscription_reports_initial_and_changed_results() {
    let schema = doctest_support::schema();
    let cfs = schema.column_families();
    let refs = cfs.iter().map(String::as_str).collect::<Vec<_>>();
    let db = doctest_support::block_on(Db::open_history_complete(DbConfig {
        schema,
        storage: doctest_support::MemoryStorage::new(&refs),
        identity: DbIdentity {
            node: NodeUuid::from_bytes([0x11; 16]),
            author: AuthorId::from_bytes([0xa1; 16]),
        },
        id_source: Some(Box::new(SeededRowIdSource::new(0x1111))),
        large_value_checkpoint_op_interval: crate::node::LARGE_VALUE_CHECKPOINT_OP_INTERVAL,
    }))
    .unwrap();
    let query = db.table("todos");
    let table = &doctest_support::schema().tables[0];
    let prepared_query = prepared(&db, &query);
    let mut subscription = doctest_support::block_on(db.subscribe(
        &prepared_query,
        ReadOpts {
            tier: DurabilityTier::Global,
            local_updates: LocalUpdates::Deferred,
            propagation: Propagation::Full,
            include_deleted: false,
            ..ReadOpts::default()
        },
    ))
    .unwrap();

    assert!(opened_rows(doctest_support::block_on(subscription.next_event()).unwrap()).is_empty());

    let todo = RowUuid::from_bytes([0x44; 16]);
    db.seed_settled_mergeable_for_bootstrap(
        "todos",
        todo,
        db.identity.author,
        doctest_support::todo_cells("subscription makes a todo appear", true),
    )
    .unwrap();

    let (added, updated, removed) =
        delta_rows(doctest_support::block_on(subscription.next_event()).unwrap());
    assert!(updated.is_empty());
    assert!(removed.is_empty());
    assert_eq!(row_ids(&added), vec![todo]);
    assert_eq!(
        added[0].cell(table, "title"),
        Some(Value::String("subscription makes a todo appear".to_owned()))
    );
    assert_eq!(added[0].cell(table, "done"), Some(Value::Bool(true)));
}

#[test]
fn db_facade_subscription_refresh_preserves_read_tier() {
    let db = doctest_support::block_on(doctest_support::open_todos_db()).unwrap();
    let query = db.table("todos");
    let prepared_query = prepared(&db, &query);
    let mut subscription = doctest_support::block_on(db.subscribe(
        &prepared_query,
        ReadOpts {
            tier: DurabilityTier::Global,
            local_updates: LocalUpdates::Deferred,
            propagation: Propagation::Full,
            include_deleted: false,
            ..ReadOpts::default()
        },
    ))
    .unwrap();

    assert!(opened_rows(doctest_support::block_on(subscription.next_event()).unwrap()).is_empty());

    db.insert(
        "todos",
        doctest_support::todo_cells("pending local-only write", true),
    )
    .unwrap();

    assert_eq!(prepared_read(&db, &query).len(), 1);
}

#[test]
fn db_facade_subscription_accepts_local_tier_for_alpha_style_live_reads() {
    let db = doctest_support::block_on(doctest_support::open_todos_db()).unwrap();
    let scheduler = Rc::new(RecordingScheduler::default());
    db.set_tick_scheduler(Some(scheduler.clone()));
    let query = db.table("todos");
    let prepared_query = prepared(&db, &query);

    let mut subscription =
        doctest_support::block_on(db.subscribe(&prepared_query, ReadOpts::default())).unwrap();
    assert_eq!(scheduler.take(), vec![TickUrgency::Immediate]);
    let opened = doctest_support::block_on(subscription.next_event()).unwrap();
    assert_eq!(opened_rows(opened), Vec::<CurrentRow>::new());

    db.insert(
        "todos",
        doctest_support::todo_cells("local callback", false),
    )
    .unwrap();
    let changed = doctest_support::block_on(subscription.next_event()).unwrap();
    let SubscriptionEvent::Delta { added, tier, .. } = changed else {
        panic!("expected local subscription delta");
    };
    assert_eq!(tier, DurabilityTier::Local);
    assert_eq!(added.len(), 1);
    assert_eq!(scheduler.take(), vec![TickUrgency::Deferred]);
}

#[test]
fn local_write_is_readable_synchronously_without_running_tick() {
    let db = doctest_support::block_on(doctest_support::open_todos_db()).unwrap();
    let scheduler = Rc::new(RecordingScheduler::default());
    db.set_tick_scheduler(Some(scheduler.clone()));
    let query = db.table("todos");
    let prepared_query = prepared(&db, &query);

    db.insert(
        "todos",
        doctest_support::todo_cells("read before tick", false),
    )
    .unwrap();

    let rows = db.read(&prepared_query).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(scheduler.take(), vec![TickUrgency::Deferred]);
}

#[test]
fn local_write_notifies_subscription_synchronously_without_running_tick() {
    let db = doctest_support::block_on(doctest_support::open_todos_db()).unwrap();
    let scheduler = Rc::new(RecordingScheduler::default());
    db.set_tick_scheduler(Some(scheduler.clone()));
    let query = db.table("todos");
    let prepared_query = prepared(&db, &query);
    let mut subscription =
        doctest_support::block_on(db.subscribe(&prepared_query, ReadOpts::default())).unwrap();
    assert_eq!(scheduler.take(), vec![TickUrgency::Immediate]);
    assert!(opened_rows(doctest_support::block_on(subscription.next_event()).unwrap()).is_empty());

    db.insert(
        "todos",
        doctest_support::todo_cells("notify before tick", false),
    )
    .unwrap();

    let (added, updated, removed) =
        delta_rows(doctest_support::block_on(subscription.next_event()).unwrap());
    assert_eq!(added.len(), 1);
    assert!(updated.is_empty());
    assert!(removed.is_empty());
    assert_eq!(scheduler.take(), vec![TickUrgency::Deferred]);
}

#[test]
fn db_facade_schedules_immediate_tick_for_attached_query_coverage() {
    let db = doctest_support::block_on(doctest_support::open_todos_db()).unwrap();
    let scheduler = Rc::new(RecordingScheduler::default());
    db.set_tick_scheduler(Some(scheduler.clone()));
    let query = db.table("todos");
    let prepared_query = prepared(&db, &query);

    db.attach_query_with_opts(
        &prepared_query,
        ReadOpts {
            tier: DurabilityTier::Global,
            local_updates: LocalUpdates::Deferred,
            propagation: Propagation::Full,
            include_deleted: false,
            ..ReadOpts::default()
        },
    )
    .unwrap();

    assert_eq!(scheduler.take(), vec![TickUrgency::Immediate]);
}

#[test]
fn db_facade_local_only_subscription_does_not_register_upstream_coverage() {
    let db = doctest_support::block_on(doctest_support::open_todos_db()).unwrap();
    let scheduler = Rc::new(RecordingScheduler::default());
    db.set_tick_scheduler(Some(scheduler.clone()));
    let query = db.table("todos");
    let prepared_query = prepared(&db, &query);

    let mut subscription = doctest_support::block_on(db.subscribe(
        &prepared_query,
        ReadOpts {
            tier: DurabilityTier::Global,
            local_updates: LocalUpdates::Deferred,
            propagation: Propagation::LocalOnly,
            include_deleted: false,
            ..ReadOpts::default()
        },
    ))
    .unwrap();

    assert!(opened_rows(doctest_support::block_on(subscription.next_event()).unwrap()).is_empty());
    assert_eq!(scheduler.take(), Vec::<TickUrgency>::new());
    assert!(db.node.upstream_subscriptions.borrow().is_empty());
}

#[test]
fn propagated_subscriptions_refcount_upstream_coverage_by_shape() {
    let db = doctest_support::block_on(doctest_support::open_todos_db()).unwrap();
    let baseline = db.runtime_stats_for_test().active_subscriptions;
    let query = db.table("todos");
    let prepared_query = prepared(&db, &query);
    let opts = ReadOpts {
        tier: DurabilityTier::Global,
        local_updates: LocalUpdates::Deferred,
        propagation: Propagation::Full,
        include_deleted: false,
        ..ReadOpts::default()
    };

    let mut first = doctest_support::block_on(db.subscribe(&prepared_query, opts.clone())).unwrap();
    let _ = doctest_support::block_on(first.next_event()).unwrap();
    assert_eq!(pending_upstream_subscribe_count(&db), 1);

    let mut second = doctest_support::block_on(db.subscribe(&prepared_query, opts)).unwrap();
    let _ = doctest_support::block_on(second.next_event()).unwrap();
    assert_eq!(
        db.runtime_stats_for_test().active_subscriptions,
        baseline + 2
    );
    assert_eq!(
        pending_upstream_subscribe_count(&db),
        1,
        "second propagating registrant should share upstream coverage"
    );

    drop(first);
    assert_eq!(
        db.runtime_stats_for_test().active_subscriptions,
        baseline + 1,
        "dropping one propagated stream must release only its local Groove output"
    );
    assert_eq!(
        pending_upstream_unsubscribe_count(&db),
        0,
        "upstream coverage stays live while another propagating registrant remains"
    );

    drop(second);
    assert_eq!(db.runtime_stats_for_test().active_subscriptions, baseline);
    assert_eq!(pending_upstream_unsubscribe_count(&db), 1);
}

#[test]
fn local_only_subscription_is_not_forwarded_on_late_upstream_connect() {
    let db = doctest_support::block_on(doctest_support::open_todos_db()).unwrap();
    let query = db.table("todos");
    let prepared_query = prepared(&db, &query);

    let mut inspector = doctest_support::block_on(db.subscribe(
        &prepared_query,
        ReadOpts {
            tier: DurabilityTier::Global,
            local_updates: LocalUpdates::Deferred,
            propagation: Propagation::LocalOnly,
            include_deleted: false,
            ..ReadOpts::default()
        },
    ))
    .unwrap();
    let _ = doctest_support::block_on(inspector.next_event()).unwrap();

    let (client_transport, _server_transport) = duplex();
    let upstream = db.connect_upstream(client_transport);
    let pending_subscribes = match &upstream.borrow().link {
        ConnectionLink::Upstream { pending, .. } => pending
            .iter()
            .filter(|command| matches!(command, PendingUpstreamCommand::Subscribe(_)))
            .count(),
        _ => unreachable!("connect_upstream creates upstream links"),
    };
    assert_eq!(pending_subscribes, 0);
}

#[test]
fn db_facade_schedules_immediate_tick_for_upstream_connection() {
    let db = doctest_support::block_on(doctest_support::open_todos_db()).unwrap();
    let scheduler = Rc::new(RecordingScheduler::default());
    db.set_tick_scheduler(Some(scheduler.clone()));
    let (client_transport, _server_transport) = duplex();

    let _upstream = db.connect_upstream(client_transport);

    assert_eq!(scheduler.take(), vec![TickUrgency::Immediate]);
}

#[test]
fn upstream_inbound_application_schedules_immediate_tick() {
    let schema = schema();
    let author = AuthorId::from_bytes([0xa1; 16]);
    let server = open_core(0x51, author, &schema);
    let client = open_db(0x52, author, &schema);
    let scheduler = Rc::new(RecordingScheduler::default());
    client.set_tick_scheduler(Some(scheduler.clone()));
    let (client_transport, server_transport) = duplex();
    let _upstream = client.connect_upstream(client_transport);
    let _subscriber = server.accept_subscriber(server_transport, author);
    scheduler.take();

    let query = client.table("todos");
    let mut subscription = prepared_subscribe(&client, &query, global_subscribe_opts()).unwrap();
    assert!(opened_rows(block_on(subscription.next_event()).unwrap()).is_empty());
    scheduler.take();

    client.tick().unwrap();
    assert!(scheduler.take().is_empty());
    server.tick().unwrap();
    assert!(scheduler.take().is_empty());
    client.tick().unwrap();

    assert_eq!(scheduler.take(), vec![TickUrgency::Immediate]);
}

#[test]
fn mergeable_tx_emits_one_subscription_delta_for_many_writes() {
    let db = doctest_support::block_on(doctest_support::open_todos_db()).unwrap();
    let query = db.table("todos");
    let prepared_query = prepared(&db, &query);
    let mut subscription =
        doctest_support::block_on(db.subscribe(&prepared_query, ReadOpts::default())).unwrap();
    assert!(opened_rows(doctest_support::block_on(subscription.next_event()).unwrap()).is_empty());

    let tx = db.mergeable_tx().unwrap();
    for index in 0..100u8 {
        tx.insert_with_id(
            "todos",
            RowUuid::from_bytes([index + 1; 16]),
            doctest_support::todo_cells(&format!("todo {index}"), false),
        )
        .unwrap();
    }
    tx.commit().unwrap();

    let (added, updated, removed) =
        delta_rows(doctest_support::block_on(subscription.next_event()).unwrap());
    assert_eq!(added.len(), 100);
    assert!(updated.is_empty());
    assert!(removed.is_empty());
    assert!(subscription.try_next_event().is_none());
}

#[test]
fn db_facade_runs_saas_shaped_local_lane_end_to_end() {
    let schema = schema();
    let dir = tempfile::tempdir().unwrap();
    let cfs = schema.column_families();
    let refs = cfs.iter().map(String::as_str).collect::<Vec<_>>();
    let storage = RocksDbStorage::open(dir.path(), &refs).unwrap();
    let owner = AuthorId::from_bytes([0xa1; 16]);
    let db = block_on(Db::open(DbConfig {
        schema: schema.clone(),
        storage,
        identity: DbIdentity {
            node: NodeUuid::from_bytes([0x11; 16]),
            author: owner,
        },
        id_source: Some(Box::new(SeededRowIdSource::new(0x11))),
        large_value_checkpoint_op_interval: crate::node::LARGE_VALUE_CHECKPOINT_OP_INTERVAL,
    }))
    .unwrap();

    let query = Query::from("todos");
    let write = db
        .insert("todos", cells("ship facade", false, owner))
        .unwrap();
    let todo = write.row_uuid();
    let table = &schema.tables[0];
    let rows = prepared_read(&db, &query);
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].cell(table, "title"),
        Some(Value::String("ship facade".to_owned()))
    );
    block_on(write.wait(DurabilityTier::Local)).unwrap();

    db.update(
        "todos",
        todo,
        BTreeMap::from([("done".to_owned(), Value::Bool(true))]),
    )
    .unwrap();
    let updated = prepared_all(&db, &query, ReadOpts::default());
    assert_eq!(updated.len(), 1);
    assert_eq!(updated[0].cell(table, "done"), Some(Value::Bool(true)));
}

/// In-memory transport pair: each side's outbound queue is the other's
/// inbound queue, so a `send` lands directly in the peer's `try_recv`.
struct DuplexTransport {
    outbound: Rc<RefCell<std::collections::VecDeque<SyncMessage>>>,
    inbound: Rc<RefCell<std::collections::VecDeque<SyncMessage>>>,
}

impl Transport for DuplexTransport {
    fn send(&mut self, message: SyncMessage) -> Result<(), TransportError> {
        self.outbound.borrow_mut().push_back(message);
        Ok(())
    }

    fn try_recv(&mut self) -> Option<SyncMessage> {
        self.inbound.borrow_mut().pop_front()
    }
}

fn duplex() -> (Box<dyn Transport>, Box<dyn Transport>) {
    use std::collections::VecDeque;
    let left = Rc::new(RefCell::new(VecDeque::new()));
    let right = Rc::new(RefCell::new(VecDeque::new()));
    (
        Box::new(DuplexTransport {
            outbound: Rc::clone(&left),
            inbound: Rc::clone(&right),
        }),
        Box::new(DuplexTransport {
            outbound: right,
            inbound: left,
        }),
    )
}

#[test]
fn permission_advice_uses_authenticated_link_identity_without_mutating() {
    let schema = owner_read_schema();
    let alice = AuthorId::from_bytes([0xa1; 16]);
    let mallory = AuthorId::from_bytes([0xb2; 16]);
    let server = open_core(0x5e, AuthorId::SYSTEM, &schema);
    let owned = server
        .insert("todos", cells("secret", false, alice))
        .unwrap()
        .row_uuid();

    let alice_client = open_db(0xa1, alice, &schema);
    let (alice_transport, alice_server_transport) = duplex();
    let _alice_upstream = alice_client.connect_upstream(alice_transport);
    let _alice_subscriber = server.accept_subscriber(alice_server_transport, alice);
    let alice_advice = alice_client.request_permission_advice(PermissionAdviceAction::Read {
        table: "todos".to_owned(),
        row: owned,
    });

    let mallory_client = open_db(0xb2, mallory, &schema);
    let (mallory_transport, mallory_server_transport) = duplex();
    let _mallory_upstream = mallory_client.connect_upstream(mallory_transport);
    let _mallory_subscriber = server.accept_subscriber(mallory_server_transport, mallory);
    let mallory_advice = mallory_client.request_permission_advice(PermissionAdviceAction::Read {
        table: "todos".to_owned(),
        row: owned,
    });

    alice_client.tick().unwrap();
    mallory_client.tick().unwrap();
    server.tick().unwrap();
    alice_client.tick().unwrap();
    mallory_client.tick().unwrap();

    assert_eq!(block_on(alice_advice), PermissionAdvice::Allowed);
    assert_eq!(block_on(mallory_advice), PermissionAdvice::Denied);
    assert_eq!(server.read(&Query::from("todos")).unwrap().len(), 1);
}

#[test]
fn permission_advice_is_unknown_until_authority_permissions_are_ready() {
    let schema = schema();
    let author = AuthorId::from_bytes([0xa1; 16]);
    let server = open_core(0x5e, AuthorId::SYSTEM, &schema);
    server.server.set_permissions_ready(false).unwrap();
    let client = open_db(0xa1, author, &schema);
    let (client_transport, server_transport) = duplex();
    let _upstream = client.connect_upstream(client_transport);
    let _subscriber = server.accept_subscriber(server_transport, author);
    let advice = client.request_permission_advice(PermissionAdviceAction::Insert {
        table: "todos".to_owned(),
        cells: cells("candidate", false, author),
    });

    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();

    assert_eq!(block_on(advice), PermissionAdvice::Unknown);
    assert!(server.read(&Query::from("todos")).unwrap().is_empty());
}

#[test]
fn partial_replica_cannot_act_as_permission_advice_authority() {
    let schema = schema();
    let author = AuthorId::from_bytes([0xa1; 16]);
    let partial = open_db(0x5e, AuthorId::SYSTEM, &schema);
    let client = open_db(0xa1, author, &schema);
    let (client_transport, partial_transport) = duplex();
    let _upstream = client.connect_upstream(client_transport);
    let _subscriber = partial.accept_subscriber(partial_transport, author);
    let advice = client.request_permission_advice(PermissionAdviceAction::Insert {
        table: "todos".to_owned(),
        cells: cells("candidate", false, author),
    });

    client.tick().unwrap();
    partial.tick().unwrap();
    client.tick().unwrap();

    assert_eq!(block_on(advice), PermissionAdvice::Unknown);
}

#[test]
fn permission_advice_update_evaluates_post_patch_update_check() {
    let policy = Query::from("todos").filter(eq(col("done"), lit(false)));
    let schema = JazzSchema::new([TableSchema::new(
        "todos",
        [
            ColumnSchema::new("title", ColumnType::String),
            ColumnSchema::new("done", ColumnType::Bool),
            ColumnSchema::new("owner", ColumnType::Uuid),
        ],
    )
    .with_read_policy(Policy::public())
    .with_write_policies(WritePolicies {
        insert_check: Policy::public(),
        update_using: Policy::public(),
        update_check: Some(policy),
        delete_using: Policy::public(),
    })]);
    let author = AuthorId::from_bytes([0xa1; 16]);
    let server = open_core(0x5e, AuthorId::SYSTEM, &schema);
    let target = server
        .insert("todos", cells("target", false, author))
        .unwrap()
        .row_uuid();
    let client = open_db(0xa1, author, &schema);
    let (client_transport, server_transport) = duplex();
    let _upstream = client.connect_upstream(client_transport);
    let _subscriber = server.accept_subscriber(server_transport, author);
    let advice = client.request_permission_advice(PermissionAdviceAction::Update {
        table: "todos".to_owned(),
        row: target,
        patch: BTreeMap::from([("done".to_owned(), Value::Bool(true))]),
    });

    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();

    assert_eq!(block_on(advice), PermissionAdvice::Denied);

    let missing = client.request_permission_advice(PermissionAdviceAction::Update {
        table: "todos".to_owned(),
        row: row(0xee),
        patch: BTreeMap::from([("done".to_owned(), Value::Bool(false))]),
    });
    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();
    assert_eq!(block_on(missing), PermissionAdvice::Denied);
}

#[test]
fn permission_advice_response_wire_cannot_carry_policy_rows_or_reasons() {
    let request_id = PermissionAdviceRequestId([7; 16]);
    let message = SyncMessage::PermissionAdviceResponse {
        request_id,
        advice: PermissionAdvice::Denied,
    };
    assert_eq!(
        message,
        SyncMessage::PermissionAdviceResponse {
            request_id,
            advice: PermissionAdvice::Denied,
        }
    );
}

#[test]
fn cancelled_permission_advice_ignores_late_or_replayed_response_ids() {
    let schema = schema();
    let author = AuthorId::from_bytes([0xa1; 16]);
    let client = open_db(0xa1, author, &schema);
    let (client_transport, mut authority_transport) = duplex();
    let _upstream = client.connect_upstream(client_transport);

    let cancelled = client.request_permission_advice(PermissionAdviceAction::Read {
        table: "todos".to_owned(),
        row: row(1),
    });
    client.tick().unwrap();
    let cancelled_id = match try_recv_subscriber_payload(authority_transport.as_mut()).unwrap() {
        SyncMessage::PermissionAdviceRequest { request_id, .. } => request_id,
        message => panic!("expected permission request, got {message:?}"),
    };
    drop(cancelled);

    let current = client.request_permission_advice(PermissionAdviceAction::Read {
        table: "todos".to_owned(),
        row: row(2),
    });
    client.tick().unwrap();
    let current_id = match try_recv_subscriber_payload(authority_transport.as_mut()).unwrap() {
        SyncMessage::PermissionAdviceRequest { request_id, .. } => request_id,
        message => panic!("expected permission request, got {message:?}"),
    };
    assert_ne!(cancelled_id, current_id);

    authority_transport
        .send(SyncMessage::PermissionAdviceResponse {
            request_id: cancelled_id,
            advice: PermissionAdvice::Denied,
        })
        .unwrap();
    authority_transport
        .send(SyncMessage::PermissionAdviceResponse {
            request_id: cancelled_id,
            advice: PermissionAdvice::Denied,
        })
        .unwrap();
    authority_transport
        .send(SyncMessage::PermissionAdviceResponse {
            request_id: current_id,
            advice: PermissionAdvice::Allowed,
        })
        .unwrap();
    client.tick().unwrap();

    assert_eq!(block_on(current), PermissionAdvice::Allowed);
}

#[test]
fn dropped_permission_advice_is_not_sent_and_reopened_nodes_use_fresh_ids() {
    let schema = schema();
    let author = AuthorId::from_bytes([0xa1; 16]);

    let first = open_db(0xa1, author, &schema);
    let (first_transport, mut first_authority) = duplex();
    let _first_upstream = first.connect_upstream(first_transport);
    let cancelled = first.request_permission_advice(PermissionAdviceAction::Insert {
        table: "todos".to_owned(),
        cells: cells("sensitive", false, author),
    });
    drop(cancelled);
    first.tick().unwrap();
    assert!(try_recv_subscriber_payload(first_authority.as_mut()).is_none());

    let first_live = first.request_permission_advice(PermissionAdviceAction::Read {
        table: "todos".to_owned(),
        row: row(1),
    });
    first.tick().unwrap();
    let first_id = match try_recv_subscriber_payload(first_authority.as_mut()).unwrap() {
        SyncMessage::PermissionAdviceRequest { request_id, .. } => request_id,
        message => panic!("expected permission request, got {message:?}"),
    };
    drop(first_live);

    let reopened = open_db(0xa1, author, &schema);
    let (reopened_transport, mut reopened_authority) = duplex();
    let _reopened_upstream = reopened.connect_upstream(reopened_transport);
    let reopened_live = reopened.request_permission_advice(PermissionAdviceAction::Read {
        table: "todos".to_owned(),
        row: row(1),
    });
    reopened.tick().unwrap();
    let reopened_id = match try_recv_subscriber_payload(reopened_authority.as_mut()).unwrap() {
        SyncMessage::PermissionAdviceRequest { request_id, .. } => request_id,
        message => panic!("expected permission request, got {message:?}"),
    };
    drop(reopened_live);

    assert_ne!(first_id, reopened_id);
}

/// Receive the next subscriber payload relevant to direct protocol assertions.
/// A subscriber begins by publishing its trusted catalogue prerequisite; tests
/// that do not model a receiving `Db` still need to consume that control-plane
/// message before asserting the requested registration/subscription response.
fn try_recv_subscriber_payload(transport: &mut dyn Transport) -> Option<SyncMessage> {
    loop {
        match transport.try_recv()? {
            SyncMessage::CatalogueSnapshot(_) => continue,
            message => return Some(message),
        }
    }
}

struct BackpressureOnceTransport {
    outbound: Rc<RefCell<std::collections::VecDeque<SyncMessage>>>,
    failed: bool,
}

impl Transport for BackpressureOnceTransport {
    fn send(&mut self, message: SyncMessage) -> Result<(), TransportError> {
        if !self.failed {
            self.failed = true;
            return Err(TransportError::Backpressure);
        }
        self.outbound.borrow_mut().push_back(message);
        Ok(())
    }

    fn try_recv(&mut self) -> Option<SyncMessage> {
        None
    }
}

/// Byte transport pair: each side sends postcard-encoded frames to the
/// other's staged inbound queue.
struct ByteDuplexTransport {
    outbound: Rc<RefCell<std::collections::VecDeque<Vec<u8>>>>,
    inbound: Rc<RefCell<std::collections::VecDeque<Vec<u8>>>>,
}

struct OneShotBackpressureTransport {
    outbound: Rc<RefCell<std::collections::VecDeque<Vec<u8>>>>,
    calls: usize,
    fail_on_call: usize,
    failed: bool,
}

impl WireTransport for OneShotBackpressureTransport {
    fn send_frame(&mut self, frame: Vec<u8>) -> Result<(), TransportError> {
        self.calls += 1;
        if self.calls == self.fail_on_call && !self.failed {
            self.failed = true;
            return Err(TransportError::Backpressure);
        }
        self.outbound.borrow_mut().push_back(frame);
        Ok(())
    }

    fn try_recv_frame(&mut self) -> Option<Vec<u8>> {
        None
    }
}

impl WireTransport for ByteDuplexTransport {
    fn send_frame(&mut self, frame: Vec<u8>) -> Result<(), TransportError> {
        self.outbound.borrow_mut().push_back(frame);
        Ok(())
    }

    fn try_recv_frame(&mut self) -> Option<Vec<u8>> {
        self.inbound.borrow_mut().pop_front()
    }
}

fn byte_duplex_raw() -> (ByteDuplexTransport, ByteDuplexTransport) {
    use std::collections::VecDeque;
    let left = Rc::new(RefCell::new(VecDeque::new()));
    let right = Rc::new(RefCell::new(VecDeque::new()));
    (
        ByteDuplexTransport {
            outbound: Rc::clone(&left),
            inbound: Rc::clone(&right),
        },
        ByteDuplexTransport {
            outbound: right,
            inbound: left,
        },
    )
}

fn byte_duplex() -> (Box<dyn Transport>, Box<dyn Transport>) {
    let (left, right) = byte_duplex_raw();
    (
        Box::new(WireTransportAdapter::current(left)),
        Box::new(WireTransportAdapter::current(right)),
    )
}

fn byte_duplex_uncompressed() -> (Box<dyn Transport>, Box<dyn Transport>) {
    let (left, right) = byte_duplex_raw();
    let features =
        FEATURE_SYNC_MESSAGE_PAYLOAD | FEATURE_STRUCTURED_ERRORS | FEATURE_MESSAGE_FRAGMENTATION;
    (
        Box::new(WireTransportAdapter::new(
            left,
            WIRE_PROTOCOL_VERSION,
            features,
            None,
        )),
        Box::new(WireTransportAdapter::new(
            right,
            WIRE_PROTOCOL_VERSION,
            features,
            None,
        )),
    )
}

#[test]
fn logical_message_larger_than_frame_round_trips_reordered_and_duplicated() {
    let (left, right) = byte_duplex_raw();
    let staged = Rc::clone(&right.inbound);
    let features =
        FEATURE_SYNC_MESSAGE_PAYLOAD | FEATURE_STRUCTURED_ERRORS | FEATURE_MESSAGE_FRAGMENTATION;
    let mut sender = WireTransportAdapter::new(left, WIRE_PROTOCOL_VERSION, features, None);
    let mut receiver = WireTransportAdapter::new(right, WIRE_PROTOCOL_VERSION, features, None);
    let body = (0..(MAX_WIRE_FRAME_BYTES + 700_000))
        .map(|index| ((index.wrapping_mul(31) % 251) as u8) as char)
        .collect::<String>();
    let message = SyncMessage::SessionClaims {
        identity: AuthorId::from_bytes([0x71; 16]),
        claims: BTreeMap::from([("large".to_owned(), Value::String(body))]),
    };

    sender.send(message.clone()).unwrap();
    let mut frames = staged.borrow_mut().drain(..).collect::<Vec<_>>();
    assert!(frames.len() > 1);
    assert!(
        frames
            .iter()
            .all(|frame| frame.len() <= MAX_WIRE_FRAME_BYTES)
    );
    frames.push(frames[0].clone());
    frames.reverse();
    staged.borrow_mut().extend(frames);

    let repeated = message.clone();
    assert_eq!(receiver.try_recv(), Some(message));
    assert!(receiver.try_recv().is_none());

    sender.send(repeated.clone()).unwrap();
    assert_eq!(receiver.try_recv(), Some(repeated));
}

#[test]
fn schema_lineage_publication_fragments_before_atomic_admission() {
    let base = schema();
    let mut evolved_schema = base.clone();
    let large_default = Value::String("x".repeat(MAX_WIRE_FRAME_BYTES + 1024));
    evolved_schema.tables[0].columns.push(
        crate::schema::ColumnSchema::new("large_default", ColumnType::String)
            .with_default(large_default.clone()),
    );
    let evolved = crate::protocol::SchemaVersion::new(evolved_schema);
    let publication = crate::protocol::SchemaLineagePublication::new(
        evolved.clone(),
        crate::protocol::MigrationLens::new(
            base.version_id(),
            evolved.id,
            vec![TableLens {
                source_table: "todos".to_owned(),
                target_table: "todos".to_owned(),
                ops: vec![LensOp::AddColumn {
                    column: "large_default".to_owned(),
                    default: large_default,
                }],
            }],
        ),
        Vec::<String>::new(),
        Vec::<String>::new(),
    );
    let message = SyncMessage::PublishSchemaWithLens {
        author: AuthorId::SYSTEM,
        catalogue_seq: 1,
        publication: Box::new(publication),
    };
    assert!(postcard::to_allocvec(&message).unwrap().len() > MAX_WIRE_FRAME_BYTES);

    let (left, right) = byte_duplex_raw();
    let staged = Rc::clone(&right.inbound);
    let features =
        FEATURE_SYNC_MESSAGE_PAYLOAD | FEATURE_STRUCTURED_ERRORS | FEATURE_MESSAGE_FRAGMENTATION;
    let mut sender = WireTransportAdapter::new(left, WIRE_PROTOCOL_VERSION, features, None);
    let mut receiver = WireTransportAdapter::new(right, WIRE_PROTOCOL_VERSION, features, None);
    sender.send(message.clone()).unwrap();
    let frames = staged.borrow_mut().drain(..).collect::<Vec<_>>();
    assert!(frames.len() > 1);
    assert!(
        frames
            .iter()
            .all(|frame| frame.len() <= MAX_WIRE_FRAME_BYTES)
    );

    let authority = open_core(0x38, AuthorId::SYSTEM, &base);
    for frame in &frames[..frames.len() - 1] {
        staged.borrow_mut().push_back(frame.clone());
        assert!(receiver.try_recv().is_none());
        assert!(
            !authority
                .node()
                .borrow()
                .catalogue_schemas()
                .contains_key(&evolved.id)
        );
    }
    staged
        .borrow_mut()
        .push_back(frames.last().unwrap().clone());
    let reassembled = receiver
        .try_recv()
        .expect("final fragment completes message");
    assert_eq!(reassembled, message);
    authority
        .node()
        .borrow_mut()
        .apply_trusted_catalogue_message(reassembled)
        .unwrap();
    assert!(
        authority
            .node()
            .borrow()
            .catalogue_schemas()
            .contains_key(&evolved.id)
    );
}

#[test]
fn corrupt_fragment_never_admits_a_partial_logical_message() {
    let (left, right) = byte_duplex_raw();
    let staged = Rc::clone(&right.inbound);
    let features =
        FEATURE_SYNC_MESSAGE_PAYLOAD | FEATURE_STRUCTURED_ERRORS | FEATURE_MESSAGE_FRAGMENTATION;
    let mut sender = WireTransportAdapter::new(left, WIRE_PROTOCOL_VERSION, features, None);
    let mut receiver = WireTransportAdapter::new(right, WIRE_PROTOCOL_VERSION, features, None);
    let message = SyncMessage::SessionClaims {
        identity: AuthorId::from_bytes([0x72; 16]),
        claims: BTreeMap::from([(
            "large".to_owned(),
            Value::String("q".repeat(MAX_WIRE_FRAME_BYTES + 64)),
        )]),
    };

    sender.send(message).unwrap();
    {
        let mut staged = staged.borrow_mut();
        let encoded = staged
            .iter_mut()
            .find(|encoded| {
                matches!(
                    decode_frame(encoded),
                    Ok(WireFrame::MessageFragment(fragment))
                        if fragment.payload.contains(&b'q')
                )
            })
            .expect("encoded string body byte exists in a fragment");
        let mut frame = decode_frame(encoded).unwrap();
        let WireFrame::MessageFragment(fragment) = &mut frame else {
            unreachable!("selected a fragment frame")
        };
        let byte = fragment
            .payload
            .iter_mut()
            .find(|byte| **byte == b'q')
            .expect("string body byte exists");
        *byte = b'r';
        *encoded = encode_frame(&frame).unwrap();
    }
    assert!(receiver.try_recv().is_none());
}

#[test]
fn fragment_admission_bounds_peer_state_and_rejects_conflicting_duplicates() {
    let features = FEATURE_SYNC_MESSAGE_PAYLOAD | FEATURE_MESSAGE_FRAGMENTATION;
    let fragment = |message_id, payload: u8| WireMessageFragment {
        protocol_version: WIRE_PROTOCOL_VERSION,
        features,
        session: None,
        message_id,
        message_digest: [payload; 32],
        total_len: 2,
        offset: 0,
        payload: vec![payload],
    };
    let mut reassembler = LogicalMessageReassembler::default();
    assert_eq!(reassembler.push(fragment(1, 1)).unwrap(), None);
    assert!(
        reassembler
            .push(fragment(1, 2))
            .unwrap_err()
            .contains("disagree")
    );
    reassembler.discard(1);
    for message_id in 0..MAX_INFLIGHT_LOGICAL_MESSAGES as u64 {
        assert_eq!(
            reassembler
                .push(fragment(message_id, message_id as u8))
                .unwrap(),
            None
        );
    }
    assert!(
        reassembler
            .push(fragment(MAX_INFLIGHT_LOGICAL_MESSAGES as u64, 9))
            .unwrap_err()
            .contains("too many incomplete")
    );
}

#[test]
fn fragmented_message_survives_mid_send_backpressure_without_semantic_retry() {
    let staged = Rc::new(RefCell::new(std::collections::VecDeque::new()));
    let receiver_inbound = Rc::clone(&staged);
    let features =
        FEATURE_SYNC_MESSAGE_PAYLOAD | FEATURE_STRUCTURED_ERRORS | FEATURE_MESSAGE_FRAGMENTATION;
    let mut sender = WireTransportAdapter::new(
        OneShotBackpressureTransport {
            outbound: staged,
            calls: 0,
            fail_on_call: 2,
            failed: false,
        },
        WIRE_PROTOCOL_VERSION,
        features,
        None,
    );
    let mut receiver = WireTransportAdapter::new(
        ByteDuplexTransport {
            outbound: Rc::new(RefCell::new(std::collections::VecDeque::new())),
            inbound: receiver_inbound,
        },
        WIRE_PROTOCOL_VERSION,
        features,
        None,
    );
    let message = SyncMessage::SessionClaims {
        identity: AuthorId::from_bytes([0x73; 16]),
        claims: BTreeMap::from([(
            "large".to_owned(),
            Value::String("b".repeat(MAX_WIRE_FRAME_BYTES + 700_000)),
        )]),
    };

    sender.send(message.clone()).unwrap();
    assert!(receiver.try_recv().is_none());
    assert!(
        sender.try_recv().is_none(),
        "poll flushes the accepted logical message"
    );
    assert_eq!(receiver.try_recv(), Some(message));
}

#[test]
fn first_frame_backpressure_queues_compressed_logical_message_without_retry() {
    let staged = Rc::new(RefCell::new(std::collections::VecDeque::new()));
    let receiver_inbound = Rc::clone(&staged);
    let features = current_wire_features();
    let mut sender = WireTransportAdapter::new(
        OneShotBackpressureTransport {
            outbound: staged,
            calls: 0,
            fail_on_call: 1,
            failed: false,
        },
        WIRE_PROTOCOL_VERSION,
        features,
        None,
    );
    let mut receiver = WireTransportAdapter::new(
        ByteDuplexTransport {
            outbound: Rc::new(RefCell::new(std::collections::VecDeque::new())),
            inbound: receiver_inbound,
        },
        WIRE_PROTOCOL_VERSION,
        features,
        None,
    );
    let message = SyncMessage::SessionClaims {
        identity: AuthorId::from_bytes([0x75; 16]),
        claims: BTreeMap::from([(
            "large".to_owned(),
            Value::String("compressible".repeat(300_000)),
        )]),
    };

    assert_eq!(sender.send(message.clone()), Ok(()));
    assert!(receiver.try_recv().is_none());
    assert!(
        sender.try_recv().is_none(),
        "poll flushes the accepted message"
    );
    assert_eq!(receiver.try_recv(), Some(message));
}

#[test]
fn reconnect_discards_missing_fragments_and_replays_the_logical_message() {
    let features =
        FEATURE_SYNC_MESSAGE_PAYLOAD | FEATURE_STRUCTURED_ERRORS | FEATURE_MESSAGE_FRAGMENTATION;
    let message = SyncMessage::SessionClaims {
        identity: AuthorId::from_bytes([0x74; 16]),
        claims: BTreeMap::from([(
            "large".to_owned(),
            Value::String("c".repeat(MAX_WIRE_FRAME_BYTES + 700_000)),
        )]),
    };

    let (left, right) = byte_duplex_raw();
    let staged = Rc::clone(&right.inbound);
    let mut sender = WireTransportAdapter::new(left, WIRE_PROTOCOL_VERSION, features, None);
    let mut receiver = WireTransportAdapter::new(right, WIRE_PROTOCOL_VERSION, features, None);
    sender.send(message.clone()).unwrap();
    staged.borrow_mut().truncate(1);
    assert!(receiver.try_recv().is_none());
    drop(receiver);

    let (left, right) = byte_duplex_raw();
    let mut sender = WireTransportAdapter::new(left, WIRE_PROTOCOL_VERSION, features, None);
    let mut receiver = WireTransportAdapter::new(right, WIRE_PROTOCOL_VERSION, features, None);
    sender.send(message.clone()).unwrap();
    assert_eq!(receiver.try_recv(), Some(message));
}

fn byte_duplex_with_session(
    identity: AuthorId,
    epoch: u64,
) -> (Box<dyn Transport>, Box<dyn Transport>) {
    let (left, right) = byte_duplex_raw();
    let session = WireSession {
        session_id: "test-session".to_owned(),
        epoch,
        identity: Some(identity),
    };
    (
        Box::new(WireTransportAdapter::new(
            left,
            WIRE_PROTOCOL_VERSION,
            FEATURE_SYNC_MESSAGE_PAYLOAD
                | crate::wire::FEATURE_SESSION_FRAME
                | FEATURE_STRUCTURED_ERRORS
                | FEATURE_MESSAGE_FRAGMENTATION,
            Some(session.clone()),
        )),
        Box::new(WireTransportAdapter::new(
            right,
            WIRE_PROTOCOL_VERSION,
            FEATURE_SYNC_MESSAGE_PAYLOAD
                | crate::wire::FEATURE_SESSION_FRAME
                | FEATURE_STRUCTURED_ERRORS
                | FEATURE_MESSAGE_FRAGMENTATION,
            Some(session),
        )),
    )
}

fn test_wire_session(identity: AuthorId, epoch: u64) -> WireSession {
    WireSession {
        session_id: "test-session".to_owned(),
        epoch,
        identity: Some(identity),
    }
}

fn test_catalogue_ack() -> SyncMessage {
    SyncMessage::CatalogueAck(crate::protocol::CatalogueAck {
        revision: Some(1),
        schema: None,
        lens: None,
        applied: true,
    })
}

fn encode_test_message_frame(session: Option<WireSession>) -> Vec<u8> {
    let payload = encode_sync_message(&test_catalogue_ack()).unwrap();
    let mut envelope = WireEnvelope::new(
        WIRE_PROTOCOL_VERSION,
        FEATURE_SYNC_MESSAGE_PAYLOAD
            | crate::wire::FEATURE_SESSION_FRAME
            | FEATURE_STRUCTURED_ERRORS,
        payload,
    );
    if let Some(session) = session {
        envelope = envelope.with_session(session);
    }
    encode_frame(&WireFrame::Message(envelope)).unwrap()
}

fn expect_auth_failed_frame(transport: &mut ByteDuplexTransport, retry: WireRetry, message: &str) {
    let error = transport.try_recv_frame().expect("structured wire error");
    let frame = decode_frame(&error).unwrap();
    let WireFrame::Error(WireError {
        code,
        retry: actual_retry,
        message: actual_message,
    }) = frame
    else {
        panic!("expected error frame");
    };
    assert_eq!(code, WireErrorCode::AuthFailed);
    assert_eq!(actual_retry, retry);
    assert!(
        actual_message.contains(message),
        "expected {actual_message:?} to contain {message:?}"
    );
}

#[test]
fn wire_transport_adapter_reports_malformed_frames() {
    let (left, mut right) = byte_duplex_raw();
    left.inbound.borrow_mut().push_back(vec![0xff, 0x00, 0x01]);

    let mut adapter = WireTransportAdapter::current(left);
    assert!(adapter.try_recv().is_none());

    let error = right.try_recv_frame().expect("structured wire error");
    let frame = decode_frame(&error).unwrap();
    assert!(matches!(
        frame,
        WireFrame::Error(WireError {
            code: WireErrorCode::MalformedFrame,
            retry: WireRetry::Never,
            ..
        })
    ));
}

#[test]
fn wire_transport_adapter_reports_oversized_frame_without_decoding() {
    let (left, mut right) = byte_duplex_raw();
    left.inbound
        .borrow_mut()
        .push_back(vec![0_u8; MAX_WIRE_FRAME_BYTES + 1]);

    let mut adapter = WireTransportAdapter::current(left);
    assert!(adapter.try_recv().is_none());

    let error = right.try_recv_frame().expect("structured wire error");
    let frame = decode_frame(&error).unwrap();
    let WireFrame::Error(WireError { code, message, .. }) = frame else {
        panic!("expected error frame");
    };
    assert_eq!(code, WireErrorCode::MalformedFrame);
    assert!(
        message.contains("wire frame size"),
        "unexpected error message: {message}"
    );
}

#[test]
fn wire_transport_adapter_accepts_matching_session() {
    let (left, mut right) = byte_duplex_raw();
    let identity = AuthorId::from_bytes([0xa1; 16]);
    let session = test_wire_session(identity, 3);
    left.inbound
        .borrow_mut()
        .push_back(encode_test_message_frame(Some(session.clone())));

    let mut adapter = WireTransportAdapter::new(
        left,
        WIRE_PROTOCOL_VERSION,
        FEATURE_SYNC_MESSAGE_PAYLOAD
            | crate::wire::FEATURE_SESSION_FRAME
            | FEATURE_STRUCTURED_ERRORS,
        Some(session),
    );

    assert_eq!(adapter.try_recv(), Some(test_catalogue_ack()));
    assert!(right.try_recv_frame().is_none());
}

#[test]
fn wire_transport_adapter_rejects_missing_session_without_emitting_sync_message() {
    let (left, mut right) = byte_duplex_raw();
    let identity = AuthorId::from_bytes([0xa2; 16]);
    left.inbound
        .borrow_mut()
        .push_back(encode_test_message_frame(None));

    let mut adapter = WireTransportAdapter::new(
        left,
        WIRE_PROTOCOL_VERSION,
        FEATURE_SYNC_MESSAGE_PAYLOAD
            | crate::wire::FEATURE_SESSION_FRAME
            | FEATURE_STRUCTURED_ERRORS,
        Some(test_wire_session(identity, 3)),
    );

    assert!(adapter.try_recv().is_none());
    expect_auth_failed_frame(&mut right, WireRetry::AfterAuth, "missing");
}

#[test]
fn fragment_authentication_precedes_reassembly_allocation() {
    let (left, mut right) = byte_duplex_raw();
    let expected_identity = AuthorId::from_bytes([0xa5; 16]);
    let features = FEATURE_SYNC_MESSAGE_PAYLOAD
        | crate::wire::FEATURE_SESSION_FRAME
        | FEATURE_STRUCTURED_ERRORS
        | FEATURE_MESSAGE_FRAGMENTATION;
    let fragment = WireMessageFragment {
        protocol_version: WIRE_PROTOCOL_VERSION,
        features,
        session: Some(test_wire_session(AuthorId::from_bytes([0xb5; 16]), 3)),
        message_id: 41,
        message_digest: [7; 32],
        total_len: MAX_LOGICAL_MESSAGE_BYTES as u64,
        offset: 0,
        payload: vec![7],
    };
    left.inbound
        .borrow_mut()
        .push_back(encode_frame(&WireFrame::MessageFragment(fragment)).unwrap());
    let mut adapter = WireTransportAdapter::new(
        left,
        WIRE_PROTOCOL_VERSION,
        features,
        Some(test_wire_session(expected_identity, 3)),
    );

    assert!(adapter.try_recv().is_none());
    assert!(adapter.reassembler.incomplete.is_empty());
    assert_eq!(adapter.reassembler.staged_bytes, 0);
    expect_auth_failed_frame(&mut right, WireRetry::AfterAuth, "identity");
}

#[test]
fn fragment_negotiation_validation_precedes_reassembly_allocation() {
    let (left, mut right) = byte_duplex_raw();
    let features = FEATURE_SYNC_MESSAGE_PAYLOAD | FEATURE_MESSAGE_FRAGMENTATION;
    let fragment = WireMessageFragment {
        protocol_version: WIRE_PROTOCOL_VERSION + 1,
        features: features | crate::wire::FEATURE_PAYLOAD_LZ4,
        session: None,
        message_id: 42,
        message_digest: [8; 32],
        total_len: MAX_LOGICAL_MESSAGE_BYTES as u64,
        offset: 0,
        payload: vec![8],
    };
    left.inbound
        .borrow_mut()
        .push_back(encode_frame(&WireFrame::MessageFragment(fragment)).unwrap());
    let mut adapter = WireTransportAdapter::new(left, WIRE_PROTOCOL_VERSION, features, None);

    assert!(adapter.try_recv().is_none());
    assert!(adapter.reassembler.incomplete.is_empty());
    assert_eq!(adapter.reassembler.staged_bytes, 0);
    let error = right.try_recv_frame().expect("structured wire error");
    assert!(matches!(
        decode_frame(&error).unwrap(),
        WireFrame::Error(WireError {
            code: WireErrorCode::UnsupportedProtocolVersion,
            ..
        })
    ));
}

#[test]
fn wire_transport_adapter_rejects_wrong_identity_without_emitting_sync_message() {
    let (left, mut right) = byte_duplex_raw();
    let expected_identity = AuthorId::from_bytes([0xa3; 16]);
    let actual_identity = AuthorId::from_bytes([0xb3; 16]);
    left.inbound
        .borrow_mut()
        .push_back(encode_test_message_frame(Some(test_wire_session(
            actual_identity,
            3,
        ))));

    let mut adapter = WireTransportAdapter::new(
        left,
        WIRE_PROTOCOL_VERSION,
        FEATURE_SYNC_MESSAGE_PAYLOAD
            | crate::wire::FEATURE_SESSION_FRAME
            | FEATURE_STRUCTURED_ERRORS,
        Some(test_wire_session(expected_identity, 3)),
    );

    assert!(adapter.try_recv().is_none());
    expect_auth_failed_frame(&mut right, WireRetry::AfterAuth, "identity");
}

#[test]
fn wire_transport_adapter_rejects_stale_epoch_without_emitting_sync_message() {
    let (left, mut right) = byte_duplex_raw();
    let identity = AuthorId::from_bytes([0xa4; 16]);
    left.inbound
        .borrow_mut()
        .push_back(encode_test_message_frame(Some(test_wire_session(
            identity, 2,
        ))));

    let mut adapter = WireTransportAdapter::new(
        left,
        WIRE_PROTOCOL_VERSION,
        FEATURE_SYNC_MESSAGE_PAYLOAD
            | crate::wire::FEATURE_SESSION_FRAME
            | FEATURE_STRUCTURED_ERRORS,
        Some(test_wire_session(identity, 3)),
    );

    assert!(adapter.try_recv().is_none());
    expect_auth_failed_frame(&mut right, WireRetry::AfterResume, "stale");
}

#[test]
fn wire_transport_adapter_preserves_message_order() {
    let (left, mut right) = byte_duplex_raw();
    let mut adapter = WireTransportAdapter::current(left);

    adapter
        .send(SyncMessage::CatalogueAck(crate::protocol::CatalogueAck {
            revision: Some(1),
            schema: None,
            lens: None,
            applied: true,
        }))
        .unwrap();
    adapter
        .send(SyncMessage::CatalogueAck(crate::protocol::CatalogueAck {
            revision: Some(2),
            schema: None,
            lens: None,
            applied: true,
        }))
        .unwrap();

    let first = right.try_recv_frame().unwrap();
    let second = right.try_recv_frame().unwrap();
    let mut decoder = WireStreamDecoder::new(current_wire_features()).unwrap();
    let first = match decode_frame(&first).unwrap() {
        WireFrame::Message(envelope) => decode_wire_message_payload(&mut decoder, &envelope),
        other => panic!("expected message frame, got {other:?}"),
    };
    let second = match decode_frame(&second).unwrap() {
        WireFrame::Message(envelope) => decode_wire_message_payload(&mut decoder, &envelope),
        other => panic!("expected message frame, got {other:?}"),
    };

    assert!(matches!(
        first,
        SyncMessage::CatalogueAck(crate::protocol::CatalogueAck {
            revision: Some(1),
            ..
        })
    ));
    assert!(matches!(
        second,
        SyncMessage::CatalogueAck(crate::protocol::CatalogueAck {
            revision: Some(2),
            ..
        })
    ));
}

#[cfg(feature = "transport-compression-lz4")]
#[test]
fn wire_transport_adapter_lz4_compresses_payload_when_negotiated() {
    let (left, right) = byte_duplex_raw();
    let mut sender = WireTransportAdapter::new(
        left,
        WIRE_PROTOCOL_VERSION,
        FEATURE_SYNC_MESSAGE_PAYLOAD | crate::wire::FEATURE_PAYLOAD_LZ4,
        None,
    );
    let mut receiver = WireTransportAdapter::new(
        right,
        WIRE_PROTOCOL_VERSION,
        FEATURE_SYNC_MESSAGE_PAYLOAD | crate::wire::FEATURE_PAYLOAD_LZ4,
        None,
    );
    let message = SyncMessage::CatalogueAck(crate::protocol::CatalogueAck {
        revision: Some(7),
        schema: None,
        lens: None,
        applied: true,
    });

    sender.send(message.clone()).unwrap();
    let raw = sender
        .into_inner()
        .outbound
        .borrow()
        .front()
        .cloned()
        .unwrap();
    let WireFrame::Message(envelope) = decode_frame(&raw).unwrap() else {
        panic!("expected message frame");
    };
    assert_eq!(
        envelope.features & crate::wire::FEATURE_PAYLOAD_LZ4,
        crate::wire::FEATURE_PAYLOAD_LZ4
    );
    assert_ne!(envelope.payload, encode_sync_message(&message).unwrap());
    assert_eq!(receiver.try_recv(), Some(message));
}

#[cfg(feature = "transport-compression-zstd")]
#[test]
fn wire_transport_adapter_zstd_stream_preserves_message_order() {
    let (left, right) = byte_duplex_raw();
    let mut sender = WireTransportAdapter::new(
        left,
        WIRE_PROTOCOL_VERSION,
        FEATURE_SYNC_MESSAGE_PAYLOAD | crate::wire::FEATURE_PAYLOAD_ZSTD,
        None,
    );
    let mut receiver = WireTransportAdapter::new(
        right,
        WIRE_PROTOCOL_VERSION,
        FEATURE_SYNC_MESSAGE_PAYLOAD | crate::wire::FEATURE_PAYLOAD_ZSTD,
        None,
    );
    let first = SyncMessage::CatalogueAck(crate::protocol::CatalogueAck {
        revision: Some(7),
        schema: None,
        lens: None,
        applied: true,
    });
    let second = SyncMessage::CatalogueAck(crate::protocol::CatalogueAck {
        revision: Some(8),
        schema: None,
        lens: None,
        applied: true,
    });

    sender.send(first.clone()).unwrap();
    sender.send(second.clone()).unwrap();

    assert_eq!(receiver.try_recv(), Some(first));
    assert_eq!(receiver.try_recv(), Some(second));
}

fn rocks_storage(schema: &JazzSchema) -> RocksDbStorage {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.keep();
    let cfs = schema.column_families();
    let refs = cfs.iter().map(String::as_str).collect::<Vec<_>>();
    RocksDbStorage::open(&path, &refs).unwrap()
}

fn open_db(node: u8, author: AuthorId, schema: &JazzSchema) -> Db<RocksDbStorage> {
    let storage = rocks_storage(schema);
    block_on(Db::open(DbConfig {
        schema: schema.clone(),
        storage,
        identity: DbIdentity {
            node: NodeUuid::from_bytes([node; 16]),
            author,
        },
        id_source: Some(Box::new(SeededRowIdSource::new(node as u64))),
        large_value_checkpoint_op_interval: crate::node::LARGE_VALUE_CHECKPOINT_OP_INTERVAL,
    }))
    .unwrap()
}

#[test]
fn live_subscription_rebuilds_when_non_genesis_permissions_head_changes() {
    let alice = AuthorId::from_bytes([0xa1; 16]);
    let bob = AuthorId::from_bytes([0xb2; 16]);
    let structural = JazzSchema::new([TableSchema::new(
        "todos",
        [
            ColumnSchema::new("title", ColumnType::String),
            ColumnSchema::new("owner", ColumnType::Uuid),
            ColumnSchema::new("editor", ColumnType::Uuid),
        ],
    )
    .with_read_policy(Policy::public())
    .with_write_policy(Policy::public())]);
    let v2_table = TableSchema::new(
        "todos",
        [
            ColumnSchema::new("title", ColumnType::String),
            ColumnSchema::new("owner", ColumnType::Uuid),
            ColumnSchema::new("editor", ColumnType::Uuid),
            ColumnSchema::new("body", ColumnType::String),
        ],
    )
    .with_write_policy(Policy::public());
    let owner_head = JazzSchema::new([v2_table
        .clone()
        .with_read_policy(Policy::owner_only("todos", "owner"))]);
    let editor_head =
        JazzSchema::new([v2_table.with_read_policy(Policy::owner_only("todos", "editor"))]);
    let owner_payload = SchemaVersion::new(owner_head.clone());
    assert_eq!(owner_payload.id, editor_head.version_id());

    let db = open_db(0xa0, AuthorId::SYSTEM, &structural);
    db.publish_schema_with_lens(
        1,
        SchemaLineagePublication::new(
            owner_payload.clone(),
            MigrationLens::new(
                structural.version_id(),
                owner_payload.id,
                vec![TableLens {
                    source_table: "todos".to_owned(),
                    target_table: "todos".to_owned(),
                    ops: vec![LensOp::AddColumn {
                        column: "body".to_owned(),
                        default: Value::String(String::new()),
                    }],
                }],
            ),
            Vec::<String>::new(),
            Vec::<String>::new(),
        ),
    )
    .unwrap();
    db.set_current_write_schema(CurrentWriteSchema {
        revision: 1,
        schema: owner_payload.id,
    })
    .unwrap();
    let first = row(0xa1);
    db.seed_settled_mergeable_for_bootstrap(
        "todos",
        first,
        AuthorId::SYSTEM,
        BTreeMap::from([
            ("title".to_owned(), Value::String("first".to_owned())),
            ("owner".to_owned(), Value::Uuid(alice.0)),
            ("editor".to_owned(), Value::Uuid(bob.0)),
            ("body".to_owned(), Value::String(String::new())),
        ]),
    )
    .unwrap();

    let prepared = db.prepare_query(&Query::from("todos")).unwrap();
    let mut subscription = block_on(db.subscribe_for_identity(
        &prepared,
        ReadOpts {
            propagation: Propagation::LocalOnly,
            ..ReadOpts::default()
        },
        alice,
    ))
    .unwrap();
    assert_eq!(
        row_ids(&opened_rows(block_on(subscription.next_event()).unwrap())),
        vec![first]
    );

    db.publish_schema(SchemaVersion::new(editor_head)).unwrap();
    db.seed_settled_mergeable_for_bootstrap(
        "todos",
        row(0xb2),
        AuthorId::SYSTEM,
        BTreeMap::from([
            ("title".to_owned(), Value::String("second".to_owned())),
            ("owner".to_owned(), Value::Uuid(bob.0)),
            ("editor".to_owned(), Value::Uuid(bob.0)),
            ("body".to_owned(), Value::String(String::new())),
        ]),
    )
    .unwrap();

    let event = subscription
        .try_next_event()
        .expect("permissions-head change must refresh the live subscription");
    let SubscriptionEvent::Delta {
        reset,
        added,
        updated,
        removed,
        ..
    } = event
    else {
        panic!("permissions-head refresh must emit a delta reset");
    };
    assert!(reset);
    assert!(added.is_empty());
    assert!(updated.is_empty());
    assert_eq!(removed.len(), 1);
    assert_eq!(removed[0].row_uuid, first);
}

fn joined_issue_query() -> Query {
    Query::from("issues").join_via("issue_tags", "issue", [eq(col("tag"), lit("prepared"))])
}

#[test]
fn prepared_query_discards_graph_handle_when_runtime_changes() {
    let schema = issue_schema();
    let db = open_db(0xb7, AuthorId::SYSTEM, &schema);
    let prepared = db.prepare_query(&joined_issue_query()).unwrap();
    let runtime_token = db.node.node.borrow().groove_runtime_token();
    assert!(
        prepared
            .plan_for_tier(DurabilityTier::Local, runtime_token)
            .is_some()
    );
    assert!(
        prepared
            .plan_for_tier(DurabilityTier::Local, runtime_token.wrapping_add(1))
            .is_none()
    );
}

fn seed_issue_project(db: &Db<RocksDbStorage>, author: AuthorId) {
    db.seed_settled_mergeable_for_bootstrap(
        "projects",
        row(10),
        author,
        BTreeMap::from([("name".to_owned(), Value::String("Platform".to_owned()))]),
    )
    .unwrap();
    db.seed_settled_mergeable_for_bootstrap(
        "issues",
        row(1),
        author,
        issue_cells("Platform", "open", author, row(10), 5, &["api"], None),
    )
    .unwrap();
    db.seed_settled_mergeable_for_bootstrap(
        "issue_tags",
        row(20),
        author,
        BTreeMap::from([
            ("issue".to_owned(), Value::Uuid(row(1).0)),
            ("tag".to_owned(), Value::String("prepared".to_owned())),
        ]),
    )
    .unwrap();
}

#[test]
fn prepared_current_write_query_installs_and_reads_non_simple_plan() {
    let schema = issue_schema();
    let author = AuthorId::from_bytes([0xa1; 16]);
    let db = open_db(0xa1, author, &schema);
    seed_issue_project(&db, author);

    let prepared = db.prepare_query(&joined_issue_query()).unwrap();
    assert!(prepared.has_plan_for_tier(DurabilityTier::Local));
    assert!(prepared.has_plan_for_tier(DurabilityTier::Global));
    db.node
        .node
        .borrow_mut()
        .clear_prepared_query_plan_cache_for_test();

    let rows = db.read(&prepared).unwrap();

    assert_eq!(row_ids(&rows), vec![row(1)]);
    assert!(
        db.node
            .node
            .borrow()
            .prepared_query_plan_cache_is_empty_for_test(),
        "stored prepared plans should be used without replanning"
    );
}

#[test]
fn subscribe_uses_prepared_non_simple_plan() {
    let schema = issue_schema();
    let author = AuthorId::from_bytes([0xa1; 16]);
    let db = open_db(0xa2, author, &schema);
    seed_issue_project(&db, author);

    let prepared = db.prepare_query(&joined_issue_query()).unwrap();
    db.node
        .node
        .borrow_mut()
        .clear_prepared_query_plan_cache_for_test();

    let mut subscription = block_on(db.subscribe(
        &prepared,
        ReadOpts {
            tier: DurabilityTier::Global,
            local_updates: LocalUpdates::Deferred,
            propagation: Propagation::Full,
            include_deleted: false,
            ..ReadOpts::default()
        },
    ))
    .unwrap();

    assert_eq!(
        row_ids(&opened_rows(block_on(subscription.next_event()).unwrap())),
        vec![row(1)]
    );
    assert!(
        db.node
            .node
            .borrow()
            .prepared_query_plan_cache_is_empty_for_test(),
        "initial subscribe read should consume the stored prepared plan"
    );
}

#[test]
fn subscription_reset_preserves_ordered_window_rank() {
    let schema = schema();
    let author = AuthorId::from_bytes([0xa1; 16]);
    let db = open_db(0xa3, author, &schema);
    for (id, title) in [(4, "alpha"), (1, "bravo"), (3, "charlie"), (2, "delta")] {
        db.seed_settled_mergeable_for_bootstrap(
            "todos",
            row(id),
            author,
            cells(title, false, author),
        )
        .unwrap();
    }

    let query = Query::from("todos")
        .order_by("title", OrderDirection::Asc)
        .offset(1)
        .limit(2);
    let mut subscription = prepared_subscribe(&db, &query, global_subscribe_opts()).unwrap();

    assert_eq!(
        row_ids(&opened_rows(block_on(subscription.next_event()).unwrap())),
        vec![row(1), row(3)],
        "reset rows must retain the selected ordered window rather than member-key order"
    );
}

#[test]
fn simple_prepared_current_write_query_uses_lowered_plan() {
    let schema = schema();
    let author = AuthorId::from_bytes([0xa1; 16]);
    let db = open_db(0xa3, author, &schema);
    db.insert_with_id("todos", row(1), cells("simple", false, author))
        .unwrap();

    let prepared = db.prepare_query(&Query::from("todos")).unwrap();
    assert!(!prepared.has_plan_for_tier(DurabilityTier::Local));
    assert!(!prepared.has_plan_for_tier(DurabilityTier::Global));

    let rows = db.read(&prepared).unwrap();

    assert_eq!(row_ids(&rows), vec![row(1)]);
    assert!(
        db.node
            .node
            .borrow()
            .prepared_query_plan_cache_is_empty_for_test(),
        "simple prepared current reads should stay on the direct lowered path without installing a shared plan"
    );
}

#[test]
fn filtered_root_prepared_query_still_reads_without_preinstalled_plan() {
    let schema = schema();
    let author = AuthorId::from_bytes([0xa1; 16]);
    let db = open_db(0xa4, author, &schema);
    db.insert_with_id("todos", row(1), cells("wanted", false, author))
        .unwrap();

    let prepared = db
        .prepare_query(&Query::from("todos").filter(eq(col("title"), lit("wanted"))))
        .unwrap();
    assert!(!prepared.has_plan_for_tier(DurabilityTier::Local));
    assert_eq!(
        db.read(&prepared)
            .unwrap()
            .into_iter()
            .map(|row| row.row_uuid())
            .collect::<Vec<_>>(),
        vec![row(1)]
    );
}

struct CoreDb {
    server: Node<RocksDbStorage>,
    schema: JazzSchema,
    author: AuthorId,
    next_now_ms: Cell<u64>,
    id_source: RefCell<SeededRowIdSource>,
}

fn open_core(node_byte: u8, author: AuthorId, schema: &JazzSchema) -> CoreDb {
    let storage = rocks_storage(schema);
    let node = NodeState::new_history_complete(
        NodeUuid::from_bytes([node_byte; 16]),
        schema.clone(),
        storage,
    )
    .unwrap();
    CoreDb {
        server: Node::new(node),
        schema: schema.clone(),
        author,
        next_now_ms: Cell::new(1),
        id_source: RefCell::new(SeededRowIdSource::new(node_byte as u64)),
    }
}

impl CoreDb {
    fn node(&self) -> Rc<RefCell<NodeState<RocksDbStorage>>> {
        self.server.node()
    }

    fn next_now_ms(&self) -> u64 {
        let next = self.next_now_ms.get();
        self.next_now_ms.set(next + 1);
        next
    }

    fn table(&self, table: impl Into<String>) -> Query {
        Query::from(table)
    }

    fn read(&self, query: &Query) -> Result<Vec<CurrentRow>, Error> {
        let shape = query.validate(&self.schema)?;
        let binding = shape.bind(BTreeMap::new())?;
        self.server
            .node()
            .borrow_mut()
            .query_rows(&shape, &binding, DurabilityTier::Local)
            .map_err(Into::into)
    }

    fn one(&self, query: &Query) -> Result<Option<CurrentRow>, Error> {
        Ok(self.read(query)?.into_iter().next())
    }

    fn at(&self, position: GlobalSeq, query: &Query) -> Result<Vec<CurrentRow>, Error> {
        let shape = query.validate(&self.schema)?;
        let binding = shape.bind(BTreeMap::new())?;
        self.server
            .node()
            .borrow_mut()
            .at(position)
            .read(&shape, &binding)
            .map_err(Into::into)
    }

    fn insert(&self, table: &str, cells: RowCells) -> Result<WriteHandle<RocksDbStorage>, Error> {
        let row = self.id_source.borrow_mut().next_row_id();
        self.insert_with_id(table, row, cells)
    }

    fn insert_with_id(
        &self,
        table: &str,
        row: RowUuid,
        cells: RowCells,
    ) -> Result<WriteHandle<RocksDbStorage>, Error> {
        let node = self.server.node();
        let tx_id = node.borrow_mut().commit_mergeable(
            MergeableCommit::new(table, row, self.next_now_ms())
                .made_by(self.author)
                .cells(cells),
        )?;
        node.borrow_mut().finalize_local_mergeable_commit(tx_id)?;
        self.server.mark_subscriber_connections_dirty();
        Ok(WriteHandle {
            node: Rc::downgrade(&node),
            row_uuid: row,
            tx_id,
            local_tier: DurabilityTier::Global,
        })
    }

    fn insert_attributed(
        &self,
        made_by: AuthorId,
        table: &str,
        cells: RowCells,
    ) -> Result<WriteHandle<RocksDbStorage>, Error> {
        let row = self.id_source.borrow_mut().next_row_id();
        let node = self.server.node();
        let tx_id = node.borrow_mut().commit_mergeable(
            MergeableCommit::new(table, row, self.next_now_ms())
                .made_by(made_by)
                .permission_subject(self.author)
                .cells(cells),
        )?;
        node.borrow_mut().finalize_local_mergeable_commit(tx_id)?;
        self.server.mark_subscriber_connections_dirty();
        Ok(WriteHandle {
            node: Rc::downgrade(&node),
            row_uuid: row,
            tx_id,
            local_tier: DurabilityTier::Global,
        })
    }

    fn update(
        &self,
        table: &str,
        row: RowUuid,
        patch: RowCells,
    ) -> Result<WriteHandle<RocksDbStorage>, Error> {
        self.update_attributed(self.author, table, row, patch)
    }

    fn update_attributed(
        &self,
        made_by: AuthorId,
        table: &str,
        row: RowUuid,
        patch: RowCells,
    ) -> Result<WriteHandle<RocksDbStorage>, Error> {
        let table_schema = self
            .schema
            .tables
            .iter()
            .find(|candidate| candidate.name == table)
            .cloned()
            .ok_or_else(|| Error::new(ErrorCode::Schema, format!("unknown table {table}")))?;
        let mut cells = BTreeMap::new();
        let mut parent = None;
        if let Some(existing) = self
            .read(&Query::from(table))?
            .into_iter()
            .find(|candidate| candidate.row_uuid() == row)
        {
            for column in &table_schema.columns {
                if let Some(value) = existing.cell(&table_schema, &column.name) {
                    cells.insert(column.name.clone(), value);
                }
            }
            parent = self.server.node().borrow_mut().current_row_tx_id(&existing);
        }
        cells.extend(patch);
        let node = self.server.node();
        let mut commit = MergeableCommit::new(table, row, self.next_now_ms())
            .made_by(made_by)
            .permission_subject(self.author)
            .cells(cells);
        if let Some(parent) = parent {
            commit = commit.parents(vec![parent]);
        }
        let tx_id = node.borrow_mut().commit_mergeable(commit)?;
        node.borrow_mut().finalize_local_mergeable_commit(tx_id)?;
        self.server.mark_subscriber_connections_dirty();
        Ok(WriteHandle {
            node: Rc::downgrade(&node),
            row_uuid: row,
            tx_id,
            local_tier: DurabilityTier::Global,
        })
    }

    fn accept_subscriber(
        &self,
        transport: Box<dyn Transport>,
        identity: AuthorId,
    ) -> Rc<RefCell<PeerConnection<RocksDbStorage>>> {
        self.server.accept_subscriber(transport, identity)
    }

    fn accept_subscriber_with_trust(
        &self,
        transport: Box<dyn Transport>,
        identity: AuthorId,
        trust: CommitUnitTrust,
    ) -> Rc<RefCell<PeerConnection<RocksDbStorage>>> {
        self.server
            .accept_subscriber_with_trust(transport, identity, trust)
    }

    fn accept_subscriber_with_claims(
        &self,
        transport: Box<dyn Transport>,
        identity: AuthorId,
        claims: BTreeMap<String, Value>,
    ) -> Rc<RefCell<PeerConnection<RocksDbStorage>>> {
        self.server
            .accept_subscriber_with_claims(transport, identity, claims)
    }

    fn accept_subscriber_with_resume(
        &self,
        transport: Box<dyn Transport>,
        identity: AuthorId,
        cursor: ResumeCursor,
    ) -> Rc<RefCell<PeerConnection<RocksDbStorage>>> {
        self.server
            .accept_subscriber_with_resume(transport, identity, cursor)
    }

    fn tick(&self) -> Result<(), Error> {
        self.server.tick().map(|_| ())
    }

    fn exclusive_tx(&self) -> Result<CoreExclusiveTx<'_>, Error> {
        let tx_id = OpenBatchId::new();
        self.server.node().borrow_mut().open_exclusive(tx_id)?;
        Ok(CoreExclusiveTx {
            core: self,
            tx_id,
            has_reads: Cell::new(false),
        })
    }

    fn publish_schema(&self, schema: SchemaVersion) -> Result<Vec<SyncMessage>, Error> {
        self.server
            .node()
            .borrow_mut()
            .apply_trusted_catalogue_message(SyncMessage::PublishSchema {
                author: self.author,
                schema: Box::new(schema),
            })
            .map_err(Into::into)
    }

    fn publish_schema_with_lens(
        &self,
        catalogue_seq: u64,
        publication: SchemaLineagePublication,
    ) -> Result<Vec<SyncMessage>, Error> {
        self.server
            .node()
            .borrow_mut()
            .apply_trusted_catalogue_message(SyncMessage::PublishSchemaWithLens {
                author: self.author,
                catalogue_seq,
                publication: Box::new(publication),
            })
            .map_err(Into::into)
    }

    fn set_current_write_schema(
        &self,
        pointer: CurrentWriteSchema,
    ) -> Result<Vec<SyncMessage>, Error> {
        self.server
            .node()
            .borrow_mut()
            .apply_trusted_catalogue_message(SyncMessage::SetCurrentWriteSchema {
                author: self.author,
                pointer,
            })
            .map_err(Into::into)
    }
}

struct CoreExclusiveTx<'a> {
    core: &'a CoreDb,
    tx_id: OpenBatchId,
    has_reads: Cell<bool>,
}

impl CoreExclusiveTx<'_> {
    fn read(&self, table: &str, row: RowUuid) -> Result<Option<RowCells>, Error> {
        self.has_reads.set(true);
        self.core
            .server
            .node()
            .borrow_mut()
            .tx_read(self.tx_id, table, row)
            .map_err(Into::into)
    }

    fn insert_with_id(&self, table: &str, row: RowUuid, cells: RowCells) -> Result<(), Error> {
        self.core
            .server
            .node()
            .borrow_mut()
            .tx_write(self.tx_id, table, row, cells, None)
            .map_err(Into::into)
    }

    fn update(&self, table: &str, row: RowUuid, patch: RowCells) -> Result<(), Error> {
        let mut cells = self.read(table, row)?.unwrap_or_default();
        cells.extend(patch);
        self.insert_with_id(table, row, cells)
    }

    fn commit(self) -> Result<TxId, Error> {
        let node = self.core.server.node();
        if self.has_reads.get() && node.borrow().open_exclusive_snapshot_moved(self.tx_id)? {
            node.borrow_mut().abandon_tx(self.tx_id)?;
            return Err(write_rejected(RejectionReason::ExclusiveConflict));
        }
        let (tx_id, unit) = node.borrow_mut().commit_exclusive(
            self.tx_id,
            self.core.author,
            self.core.next_now_ms(),
        )?;
        let SyncMessage::CommitUnit { tx, versions } = unit else {
            return Err(Error::new(
                ErrorCode::Protocol,
                "commit_exclusive must yield a CommitUnit",
            ));
        };
        let fate = node
            .borrow_mut()
            .finalize_local_exclusive_commit(tx, versions)?;
        if let Fate::Rejected(reason) = fate {
            return Err(write_rejected(reason));
        }
        self.core.server.mark_subscriber_connections_dirty();
        Ok(tx_id)
    }
}

/// Commit a row on an authority node and confirm it reached Global, so the
/// serving path ships it.
fn seed(db: &CoreDb, table: &str, cells: RowCells) -> RowUuid {
    let write = db.insert(table, cells).unwrap();
    block_on(write.wait(DurabilityTier::Global)).unwrap();
    write.row_uuid()
}

#[test]
fn db_at_reads_historical_cut_and_partial_requires_server() {
    let schema = schema();
    let author = AuthorId::from_bytes([0xa1; 16]);
    let core = open_core(0x5e, AuthorId::SYSTEM, &schema);
    let partial = open_db(0xc1, author, &schema);
    let todo = row(0x42);

    core.insert_with_id("todos", todo, cells("draft", false, author))
        .unwrap();
    core.update(
        "todos",
        todo,
        BTreeMap::from([("title".to_owned(), Value::String("final".to_owned()))]),
    )
    .unwrap();

    let table = &schema.tables[0];
    let at_first = core.at(GlobalSeq(1), &Query::from("todos")).unwrap();
    assert_eq!(at_first.len(), 1);
    assert_eq!(
        at_first[0].cell(table, "title"),
        Some(Value::String("draft".to_owned()))
    );
    let at_second = core.at(GlobalSeq(2), &Query::from("todos")).unwrap();
    assert_eq!(
        at_second[0].cell(table, "title"),
        Some(Value::String("final".to_owned()))
    );

    let partial_todos = partial.prepare_query(&Query::from("todos")).unwrap();
    let err = partial.at(GlobalSeq(1), &partial_todos).unwrap_err();
    assert_eq!(err.code, ErrorCode::HistoricalReadRequiresServer);
    assert_eq!(err.message, "historical read requires server evaluation");
}

#[test]
fn db_catalogue_facade_publishes_schema_lens_and_current_write_schema() {
    let base = owner_write_schema();
    let evolved = evolved_owner_write_schema();
    let owner = AuthorId::from_bytes([0xa1; 16]);
    let core = open_core(0x5e, AuthorId::SYSTEM, &base);
    let client = open_db(0xc1, owner, &base);
    let schema_version = SchemaVersion::new(evolved.clone());

    let lens = MigrationLens::new(
        base.version_id(),
        schema_version.id,
        vec![TableLens {
            source_table: "todos".to_owned(),
            target_table: "todos".to_owned(),
            ops: vec![LensOp::AddColumn {
                column: "body".to_owned(),
                default: Value::String(String::new()),
            }],
        }],
    );
    let lens_ack = core
        .publish_schema_with_lens(
            1,
            SchemaLineagePublication::new(
                schema_version.clone(),
                lens.clone(),
                Vec::<String>::new(),
                Vec::<String>::new(),
            ),
        )
        .unwrap();
    assert!(matches!(
        lens_ack.as_slice(),
        [SyncMessage::CatalogueAck(ack)]
            if ack.schema == Some(schema_version.id)
                && ack.lens == Some(lens.id)
                && ack.applied
    ));

    let pointer = CurrentWriteSchema {
        revision: 2,
        schema: schema_version.id,
    };
    let pointer_ack = core.set_current_write_schema(pointer).unwrap();
    assert!(matches!(
        pointer_ack.as_slice(),
        [SyncMessage::CatalogueAck(ack)] if ack.revision == Some(2) && ack.schema == Some(schema_version.id) && ack.applied
    ));

    let row = seed(&core, "todos", cells("under evolved schema", false, owner));
    let rows = core.read(&Query::from("todos")).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].row_uuid(), row);

    let unauthorized = client.publish_schema(schema_version).unwrap_err();
    assert_eq!(unauthorized.code, ErrorCode::Protocol);
    assert!(
        unauthorized
            .message
            .contains("catalogue updates require a serving Node")
    );
}

#[test]
fn core_db_self_finalizes_own_writes_to_global() {
    let schema = schema();
    let owner = AuthorId::from_bytes([0xa1; 16]);
    let core = open_core(0x5e, AuthorId::SYSTEM, &schema);

    let write = core
        .insert("todos", cells("authority write", false, owner))
        .unwrap();
    // No upstream, no connection: a Core Db is the authority, so its own
    // write is immediately Accepted/Global.
    assert_eq!(
        block_on(write.wait(DurabilityTier::Global)).unwrap(),
        write.mergeable_tx_id()
    );
    assert_eq!(core.read(&Query::from("todos")).unwrap().len(), 1);
}

#[test]
fn db_sync_surface_round_trips_subscription_to_client() {
    let schema = schema();
    let owner = AuthorId::from_bytes([0xa1; 16]);
    let client_author = AuthorId::from_bytes([0xc1; 16]);

    let server = open_core(0x5e, AuthorId::SYSTEM, &schema);
    let client = open_db(0xc1, client_author, &schema);

    seed(&server, "todos", cells("from server", false, owner));

    // Wire the two Dbs together and subscribe on the client.
    let (client_transport, server_transport) = duplex();
    let _upstream = client.connect_upstream(client_transport);
    let _subscriber = server.accept_subscriber(server_transport, client_author);

    let query = Query::from("todos");
    let mut subscription = prepared_subscribe(&client, &query, global_subscribe_opts()).unwrap();
    let opened = block_on(subscription.next_event()).unwrap();
    assert!(!event_settled(&opened));
    assert!(opened_rows(opened).is_empty());

    // Drive: client announces the shape -> server serves -> client applies.
    client.tick().unwrap(); // RegisterShape + Subscribe upstream
    server.tick().unwrap(); // ViewUpdate downstream
    client.tick().unwrap(); // apply, push the subscription event

    let table = &schema.tables[0];
    let rows = prepared_read(&client, &query);
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].cell(table, "title"),
        Some(Value::String("from server".to_owned()))
    );
    let (added, updated, removed) = delta_rows(block_on(subscription.next_event()).unwrap());
    assert_eq!(added.len(), 1);
    assert!(updated.is_empty());
    assert!(removed.is_empty());

    // A later server write propagates incrementally on the next round trip.
    seed(&server, "todos", cells("second", true, owner));
    server.tick().unwrap();
    client.tick().unwrap();
    assert_eq!(prepared_read(&client, &query).len(), 2);
}

#[test]
fn large_logical_snapshot_crosses_byte_peer_transport_and_settles() {
    let schema = schema();
    let owner = AuthorId::from_bytes([0x71; 16]);
    let client_author = AuthorId::from_bytes([0x72; 16]);
    let server = open_core(0x73, AuthorId::SYSTEM, &schema);
    let client = open_db(0x74, client_author, &schema);
    let expected = 900;

    for idx in 0..expected {
        seed(
            &server,
            "todos",
            cells(&format!("row-{idx}-{}", "x".repeat(4096)), false, owner),
        );
    }

    let (client_transport, server_transport) = byte_duplex_uncompressed();
    let _upstream = client.connect_upstream(client_transport);
    let _subscriber = server.accept_subscriber(server_transport, client_author);

    let query = Query::from("todos");
    let mut subscription = prepared_subscribe(&client, &query, global_subscribe_opts()).unwrap();
    let opened = block_on(subscription.next_event()).unwrap();
    assert!(!event_settled(&opened));
    assert!(opened_rows(opened).is_empty());

    for _ in 0..200 {
        client.tick().unwrap();
        server.tick().unwrap();
        client.tick().unwrap();

        while let Some(event) = subscription.try_next_event() {
            let settled = event_settled(&event);
            let snapshot = snapshot_from_event(event);
            if settled {
                assert_eq!(snapshot.rows.len(), expected);
                return;
            }
        }
    }

    let rows = prepared_read(&client, &query);
    panic!(
        "large logical snapshot subscription did not settle; currently visible rows={}",
        rows.len()
    );
}

#[test]
fn offline_branch_creation_and_commit_sync_metadata_before_data() {
    let schema = schema();
    let identity = AuthorId::from_bytes([0xc1; 16]);
    let server = open_core(0x5e, AuthorId::SYSTEM, &schema);
    let client = open_db(0xc1, identity, &schema);
    let branch = BranchId::from_bytes([0x42; 16]);
    client.create_branch_with_id(branch).unwrap();
    let write = client
        .insert_on_branch(branch, "todos", cells("offline branch", false, identity))
        .unwrap();
    let branch_row = write.row_uuid();
    assert!(server.node().borrow().branch_record(branch).is_none());

    let (client_transport, server_transport) = duplex();
    let _upstream = client.connect_upstream(client_transport);
    let _subscriber = server.accept_subscriber(server_transport, identity);
    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();

    let record = client
        .node
        .node
        .borrow()
        .branch_record(branch)
        .cloned()
        .unwrap();
    assert_eq!(record.created_by, identity);
    let received = server
        .node()
        .borrow()
        .branch_record(branch)
        .cloned()
        .unwrap();
    assert_eq!(received.branch_id, record.branch_id);
    assert_eq!(received.created_by, record.created_by);
    assert_eq!(received.parent, record.parent);
    assert_eq!(
        received.base.as_ref().map(|base| base.global_base),
        record.base.as_ref().map(|base| base.global_base)
    );
    assert_eq!(
        server
            .node()
            .borrow_mut()
            .transaction_record(write.mergeable_tx_id())
            .unwrap()
            .target_lineage,
        crate::tx::BranchLineage::Branch(branch)
    );
    let shape = Query::from("todos").validate(&schema).unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    let rows = server
        .node()
        .borrow_mut()
        .query_rows_on_branch(branch, &shape, &binding)
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].row_uuid(), branch_row);
}

#[test]
fn session_branch_metadata_rejects_creator_mismatch() {
    let schema = schema();
    let identity = AuthorId::from_bytes([0xc1; 16]);
    let server = open_core(0x5e, AuthorId::SYSTEM, &schema);
    let branch = BranchId::from_bytes([0x42; 16]);
    let (mut client_transport, server_transport) = duplex();
    let subscriber = server.accept_subscriber(server_transport, identity);

    client_transport
        .send(SyncMessage::BranchMetadata(BranchMetadata {
            branch_id: branch,
            created_by: AuthorId::from_bytes([0xee; 16]),
            parent: Some(BranchId::from_bytes([0xdd; 16])),
            base: None,
            open: false,
        }))
        .unwrap();
    assert!(subscriber.borrow_mut().tick().is_err());
    assert!(server.node().borrow().branch_record(branch).is_none());
}

#[test]
fn session_branch_metadata_rejects_malformed_initial_shapes() {
    let schema = schema();
    let identity = AuthorId::from_bytes([0xc1; 16]);
    let source = open_core(0xc1, identity, &schema);
    let branch = BranchId::from_bytes([0x49; 16]);
    let record = source
        .node()
        .borrow_mut()
        .create_branch_as(branch, identity)
        .unwrap();
    let canonical = BranchMetadata::from(&record);
    let mut discarded = canonical.clone();
    discarded.open = false;
    let mut parented = canonical.clone();
    parented.parent = Some(BranchId::from_bytes([0xdd; 16]));
    let mut arbitrary_owner = canonical.clone();
    arbitrary_owner.base.as_mut().unwrap().owner = NodeUuid::from_bytes([0xee; 16]);
    let mut local_tail = canonical.clone();
    local_tail.base.as_mut().unwrap().local_base = TxTime(1);
    let mut dotted = canonical;
    dotted
        .base
        .as_mut()
        .unwrap()
        .dots
        .push(TxId::new(TxTime(1), NodeUuid(uuid::Uuid::nil())));

    for metadata in [discarded, parented, arbitrary_owner, local_tail, dotted] {
        let server = open_core(0x5e, AuthorId::SYSTEM, &schema);
        let (mut client_transport, server_transport) = duplex();
        let subscriber = server.accept_subscriber(server_transport, identity);
        client_transport
            .send(SyncMessage::BranchMetadata(metadata))
            .unwrap();
        assert!(subscriber.borrow_mut().tick().is_err());
        assert!(server.node().borrow().branch_record(branch).is_none());
    }
}

#[test]
fn empty_branch_metadata_retries_after_unacked_reopen() {
    let schema = schema();
    let identity = AuthorId::from_bytes([0xc1; 16]);
    let node_uuid = NodeUuid::from_bytes([0xc1; 16]);
    let branch = BranchId::from_bytes([0x4a; 16]);
    let dir = tempfile::tempdir().unwrap();
    let cfs = schema.column_families();
    let refs = cfs.iter().map(String::as_str).collect::<Vec<_>>();
    let storage = RocksDbStorage::open(dir.path(), &refs).unwrap();
    let client = block_on(Db::open(DbConfig {
        schema: schema.clone(),
        storage,
        identity: DbIdentity {
            node: node_uuid,
            author: identity,
        },
        id_source: None,
        large_value_checkpoint_op_interval: crate::node::LARGE_VALUE_CHECKPOINT_OP_INTERVAL,
    }))
    .unwrap();
    client.create_branch_with_id(branch).unwrap();
    let first_server = open_core(0x5e, AuthorId::SYSTEM, &schema);
    let (client_transport, server_transport) = duplex();
    let upstream = client.connect_upstream(client_transport);
    let _subscriber = first_server.accept_subscriber(server_transport, identity);
    upstream.borrow_mut().tick().unwrap();
    first_server.tick().unwrap();
    assert!(first_server.node().borrow().branch_record(branch).is_some());
    drop(upstream);
    client.close().unwrap();
    drop(client);

    let storage = RocksDbStorage::open(dir.path(), &refs).unwrap();
    let reopened = block_on(Db::open(DbConfig {
        schema,
        storage,
        identity: DbIdentity {
            node: node_uuid,
            author: identity,
        },
        id_source: None,
        large_value_checkpoint_op_interval: crate::node::LARGE_VALUE_CHECKPOINT_OP_INTERVAL,
    }))
    .unwrap();
    assert_eq!(
        reopened
            .node
            .node
            .borrow()
            .pending_branch_metadata_uploads()
            .len(),
        1
    );
    let replay_server = open_core(0x6e, AuthorId::SYSTEM, &reopened.schema);
    let (client_transport, server_transport) = duplex();
    let upstream = reopened.connect_upstream(client_transport);
    let _subscriber = replay_server.accept_subscriber(server_transport, identity);
    upstream.borrow_mut().tick().unwrap();
    replay_server.tick().unwrap();
    upstream.borrow_mut().tick().unwrap();
    assert!(
        replay_server
            .node()
            .borrow()
            .branch_record(branch)
            .is_some()
    );
    assert!(
        reopened
            .node
            .node
            .borrow()
            .pending_branch_metadata_uploads()
            .is_empty()
    );
}

#[test]
fn acknowledged_open_accepts_remote_discard_and_recovers_it() {
    let schema = schema();
    let identity = AuthorId::from_bytes([0xc1; 16]);
    let node_uuid = NodeUuid::from_bytes([0xc2; 16]);
    let branch = BranchId::from_bytes([0x4d; 16]);
    let dir = tempfile::tempdir().unwrap();
    let cfs = schema.column_families();
    let refs = cfs.iter().map(String::as_str).collect::<Vec<_>>();
    let storage = RocksDbStorage::open(dir.path(), &refs).unwrap();
    let client = block_on(Db::open(DbConfig {
        schema: schema.clone(),
        storage,
        identity: DbIdentity {
            node: node_uuid,
            author: identity,
        },
        id_source: None,
        large_value_checkpoint_op_interval: crate::node::LARGE_VALUE_CHECKPOINT_OP_INTERVAL,
    }))
    .unwrap();
    let authority = open_core(0x5e, AuthorId::SYSTEM, &schema);
    client.create_branch_with_id(branch).unwrap();
    let (client_transport, authority_transport) = duplex();
    let upstream = client.connect_upstream(client_transport);
    let subscriber = authority.accept_subscriber(authority_transport, identity);
    client.tick().unwrap();
    authority.tick().unwrap();
    client.tick().unwrap();
    assert!(
        client
            .node
            .node
            .borrow()
            .pending_branch_metadata_uploads()
            .is_empty()
    );
    drop(upstream);
    drop(subscriber);

    authority
        .node()
        .borrow_mut()
        .discard_branch(branch)
        .unwrap();
    let discarded = BranchMetadata::from(authority.node().borrow().branch_record(branch).unwrap());
    assert!(!discarded.open);
    let (client_transport, mut trusted_remote) = duplex();
    let upstream = client.connect_upstream(client_transport);
    trusted_remote
        .send(SyncMessage::BranchMetadata(discarded.clone()))
        .unwrap();
    upstream.borrow_mut().tick().unwrap();
    assert_eq!(
        BranchMetadata::from(client.node.node.borrow().branch_record(branch).unwrap()),
        discarded
    );
    drop(upstream);
    client.close().unwrap();
    drop(client);

    let storage = RocksDbStorage::open(dir.path(), &refs).unwrap();
    let reopened = block_on(Db::open(DbConfig {
        schema,
        storage,
        identity: DbIdentity {
            node: node_uuid,
            author: identity,
        },
        id_source: None,
        large_value_checkpoint_op_interval: crate::node::LARGE_VALUE_CHECKPOINT_OP_INTERVAL,
    }))
    .unwrap();
    assert_eq!(
        BranchMetadata::from(reopened.node.node.borrow().branch_record(branch).unwrap()),
        discarded
    );
}

#[test]
fn edge_durably_relays_empty_branch_creation_and_discard_after_reopen() {
    let schema = schema();
    let identity = AuthorId::from_bytes([0xc1; 16]);
    let edge_uuid = NodeUuid::from_bytes([0xe1; 16]);
    let branch = BranchId::from_bytes([0x4c; 16]);
    let client = open_db(0xc1, identity, &schema);
    let authority = open_core(0x5e, AuthorId::SYSTEM, &schema);
    let edge_dir = tempfile::tempdir().unwrap();
    let cfs = schema.column_families();
    let refs = cfs.iter().map(String::as_str).collect::<Vec<_>>();

    let edge_storage = RocksDbStorage::open(edge_dir.path(), &refs).unwrap();
    let edge = Node::new(
        NodeState::new_history_complete(edge_uuid, schema.clone(), edge_storage).unwrap(),
    );
    client.create_branch_with_id(branch).unwrap();
    let (client_transport, edge_transport) = duplex();
    let client_link = client.connect_upstream(client_transport);
    let edge_downstream = edge.accept_subscriber(edge_transport, identity);
    client.tick().unwrap();
    edge.tick().unwrap();
    client.tick().unwrap();
    assert_eq!(
        edge.node().borrow().pending_branch_metadata_uploads().len(),
        1
    );
    assert!(authority.node().borrow().branch_record(branch).is_none());
    drop(client_link);
    drop(edge_downstream);
    drop(edge);

    // The edge acknowledged the client hop, but its independent authority hop
    // remains durable across restart.
    let edge_storage = RocksDbStorage::open(edge_dir.path(), &refs).unwrap();
    let edge = Node::new(
        NodeState::new_history_complete(edge_uuid, schema.clone(), edge_storage).unwrap(),
    );
    assert_eq!(
        edge.node().borrow().pending_branch_metadata_uploads().len(),
        1
    );
    let (edge_transport, authority_transport) = duplex();
    let edge_upstream = edge.connect_upstream(edge_transport);
    let authority_downstream = authority.accept_subscriber_with_trust(
        authority_transport,
        identity,
        CommitUnitTrust::TrustedBackend,
    );
    edge.tick().unwrap();
    authority.tick().unwrap();
    edge.tick().unwrap();
    assert!(authority.node().borrow().branch_record(branch).is_some());
    assert!(
        edge.node()
            .borrow()
            .pending_branch_metadata_uploads()
            .is_empty()
    );
    drop(edge_upstream);
    drop(authority_downstream);

    // A delayed exact retry from the downstream author is acknowledged but
    // does not reopen an already-acknowledged upstream relay.
    let open_metadata =
        BranchMetadata::from(client.node.node.borrow().branch_record(branch).unwrap());
    let (mut retry_transport, edge_transport) = duplex();
    let retry_downstream = edge.accept_subscriber(edge_transport, identity);
    retry_transport
        .send(SyncMessage::BranchMetadata(open_metadata))
        .unwrap();
    retry_downstream.borrow_mut().tick().unwrap();
    assert!(
        edge.node()
            .borrow()
            .pending_branch_metadata_uploads()
            .is_empty()
    );
    drop(retry_downstream);

    client
        .node
        .node
        .borrow_mut()
        .discard_branch(branch)
        .unwrap();
    let (client_transport, edge_transport) = duplex();
    let client_link = client.connect_upstream(client_transport);
    let edge_downstream = edge.accept_subscriber(edge_transport, identity);
    client.tick().unwrap();
    edge.tick().unwrap();
    client.tick().unwrap();
    assert_eq!(
        edge.node().borrow().pending_branch_metadata_uploads().len(),
        1
    );
    assert!(BranchMetadata::from(authority.node().borrow().branch_record(branch).unwrap()).open);
    drop(client_link);
    drop(edge_downstream);
    drop(edge);

    let edge_storage = RocksDbStorage::open(edge_dir.path(), &refs).unwrap();
    let edge = Node::new(
        NodeState::new_history_complete(edge_uuid, schema.clone(), edge_storage).unwrap(),
    );
    assert_eq!(
        edge.node().borrow().pending_branch_metadata_uploads().len(),
        1
    );
    let (edge_transport, authority_transport) = duplex();
    let _edge_upstream = edge.connect_upstream(edge_transport);
    let _authority_downstream = authority.accept_subscriber_with_trust(
        authority_transport,
        identity,
        CommitUnitTrust::TrustedBackend,
    );
    edge.tick().unwrap();
    authority.tick().unwrap();
    edge.tick().unwrap();
    assert!(!BranchMetadata::from(authority.node().borrow().branch_record(branch).unwrap()).open);
    assert!(
        edge.node()
            .borrow()
            .pending_branch_metadata_uploads()
            .is_empty()
    );
}

#[test]
fn session_branch_data_parks_until_authenticated_metadata_arrives() {
    let schema = schema();
    let identity = AuthorId::from_bytes([0xc1; 16]);
    let server = open_core(0x5e, AuthorId::SYSTEM, &schema);
    let writer = open_core(0xc1, identity, &schema);
    let branch = BranchId::from_bytes([0x47; 16]);
    let record = writer
        .node()
        .borrow_mut()
        .create_branch_as(branch, identity)
        .unwrap();
    let tx_id = writer
        .node()
        .borrow_mut()
        .commit_mergeable_on_branch(
            branch,
            MergeableCommit::new("todos", row(0x47), 1)
                .made_by(identity)
                .cells(cells("data first", false, identity)),
        )
        .unwrap();
    let unit = writer.node().borrow_mut().commit_unit_for(tx_id).unwrap();
    let (mut client_transport, server_transport) = duplex();
    let subscriber = server.accept_subscriber(server_transport, identity);

    client_transport.send(unit).unwrap();
    subscriber.borrow_mut().tick().unwrap();
    assert!(
        server
            .node()
            .borrow_mut()
            .transaction_record(tx_id)
            .is_none()
    );
    assert!(matches!(
        try_recv_subscriber_payload(client_transport.as_mut()),
        Some(SyncMessage::FetchBranchMetadata { branches }) if branches == vec![branch]
    ));

    client_transport
        .send(SyncMessage::BranchMetadata((&record).into()))
        .unwrap();
    subscriber.borrow_mut().tick().unwrap();
    assert_eq!(
        server
            .node()
            .borrow_mut()
            .transaction_record(tx_id)
            .unwrap()
            .target_lineage,
        crate::tx::BranchLineage::Branch(branch)
    );
}

#[test]
fn session_branch_metadata_parks_until_snapshot_base_arrives() {
    let schema = schema();
    let identity = AuthorId::from_bytes([0xc1; 16]);
    let source = open_core(0xc1, identity, &schema);
    let server = open_core(0x5e, AuthorId::SYSTEM, &schema);
    let base_write = source
        .insert("todos", cells("base first", false, identity))
        .unwrap();
    let base_unit = source
        .node()
        .borrow_mut()
        .commit_unit_for(base_write.mergeable_tx_id())
        .unwrap();
    let branch = BranchId::from_bytes([0x48; 16]);
    let record = source
        .node()
        .borrow_mut()
        .create_branch_as(branch, identity)
        .unwrap();
    assert_eq!(record.base.as_ref().unwrap().global_base, GlobalSeq(1));
    let (mut client_transport, server_transport) = duplex();
    let subscriber = server.accept_subscriber(server_transport, identity);

    client_transport
        .send(SyncMessage::BranchMetadata((&record).into()))
        .unwrap();
    subscriber.borrow_mut().tick().unwrap();
    assert!(server.node().borrow().branch_record(branch).is_none());

    client_transport.send(base_unit).unwrap();
    subscriber.borrow_mut().tick().unwrap();
    subscriber.borrow_mut().tick().unwrap();
    assert!(server.node().borrow().branch_record(branch).is_some());
}

#[test]
fn locally_created_branch_and_commit_survive_rocks_reopen() {
    let schema = schema();
    let identity = AuthorId::from_bytes([0xc1; 16]);
    let node_uuid = NodeUuid::from_bytes([0xc1; 16]);
    let branch = BranchId::from_bytes([0x43; 16]);
    let dir = tempfile::tempdir().unwrap();
    let cfs = schema.column_families();
    let refs = cfs.iter().map(String::as_str).collect::<Vec<_>>();
    let storage = RocksDbStorage::open(dir.path(), &refs).unwrap();
    let client = block_on(Db::open(DbConfig {
        schema: schema.clone(),
        storage,
        identity: DbIdentity {
            node: node_uuid,
            author: identity,
        },
        id_source: Some(Box::new(SeededRowIdSource::new(0xc1))),
        large_value_checkpoint_op_interval: crate::node::LARGE_VALUE_CHECKPOINT_OP_INTERVAL,
    }))
    .unwrap();
    client.create_branch_with_id(branch).unwrap();
    let write = client
        .insert_on_branch(branch, "todos", cells("durable offline", false, identity))
        .unwrap();
    let tx_id = write.mergeable_tx_id();
    let expected = client
        .node
        .node
        .borrow()
        .branch_record(branch)
        .cloned()
        .unwrap();
    client.close().unwrap();
    drop(client);
    let storage = RocksDbStorage::open(dir.path(), &refs).unwrap();
    let reopened = block_on(Db::open(DbConfig {
        schema,
        storage,
        identity: DbIdentity {
            node: node_uuid,
            author: identity,
        },
        id_source: Some(Box::new(SeededRowIdSource::new(0xc2))),
        large_value_checkpoint_op_interval: crate::node::LARGE_VALUE_CHECKPOINT_OP_INTERVAL,
    }))
    .unwrap();
    assert_eq!(
        reopened.node.node.borrow().branch_record(branch),
        Some(&expected)
    );
    assert!(reopened.write_state(tx_id).is_ok());

    // Recovery restores both independent durable outboxes: metadata must be
    // replayed and admitted before the branch-target transaction can land.
    let server = open_core(0x5e, AuthorId::SYSTEM, &reopened.schema);
    let (client_transport, server_transport) = duplex();
    let _upstream = reopened.connect_upstream(client_transport);
    let _subscriber = server.accept_subscriber(server_transport, identity);
    reopened.tick().unwrap();
    server.tick().unwrap();
    reopened.tick().unwrap();
    server.tick().unwrap();
    assert_eq!(
        server.node().borrow().branch_record(branch),
        Some(&expected)
    );
    assert_eq!(
        server
            .node()
            .borrow_mut()
            .transaction_record(tx_id)
            .unwrap()
            .target_lineage,
        crate::tx::BranchLineage::Branch(branch)
    );
}

#[test]
fn trusted_branch_snapshot_round_trips_without_receiver_reauthoring() {
    let schema = schema();
    let backend_identity = AuthorId::from_bytes([0xb0; 16]);
    let receiver_uuid = NodeUuid::from_bytes([0x5e; 16]);
    let snapshot_owner = NodeUuid::from_bytes([0xa7; 16]);
    let branch = BranchId::from_bytes([0x4b; 16]);
    let snapshot = crate::tx::Snapshot::exclusive_base(
        snapshot_owner,
        GlobalSeq(0),
        TxTime(7),
        vec![TxId::new(TxTime(8), snapshot_owner)],
    )
    .unwrap();
    let metadata = BranchMetadata {
        branch_id: branch,
        created_by: backend_identity,
        parent: None,
        base: Some(snapshot.clone()),
        open: true,
    };
    let dir = tempfile::tempdir().unwrap();
    let cfs = schema.column_families();
    let refs = cfs.iter().map(String::as_str).collect::<Vec<_>>();
    let storage = RocksDbStorage::open(dir.path(), &refs).unwrap();
    let target =
        Node::new(NodeState::new_history_complete(receiver_uuid, schema.clone(), storage).unwrap());
    let (mut backend_transport, server_transport) = duplex();
    let subscriber = target.accept_subscriber_with_trust(
        server_transport,
        backend_identity,
        CommitUnitTrust::TrustedBackend,
    );
    backend_transport
        .send(SyncMessage::BranchMetadata(metadata.clone()))
        .unwrap();
    subscriber.borrow_mut().tick().unwrap();
    assert_eq!(
        target.node().borrow().branch_record(branch).unwrap().base,
        Some(snapshot.clone())
    );

    drop(subscriber);
    drop(target);
    let storage = RocksDbStorage::open(dir.path(), &refs).unwrap();
    let reopened = NodeState::new_history_complete(receiver_uuid, schema, storage).unwrap();
    assert_eq!(
        BranchMetadata::from(reopened.branch_record(branch).unwrap()),
        metadata
    );
}

#[test]
fn trusted_backend_replays_branch_metadata_over_transport() {
    // Internal trust-boundary test: raw routing metadata is intentionally only
    // accepted on a trusted backend transport and has no public client facade.
    let schema = schema();
    let backend_identity = AuthorId::from_bytes([0xb0; 16]);
    let source = open_core(0xb0, AuthorId::SYSTEM, &schema);
    let target = open_core(0x5e, AuthorId::SYSTEM, &schema);
    let branch = BranchId::from_bytes([0x44; 16]);
    let record = source
        .node()
        .borrow_mut()
        .create_branch_as(branch, backend_identity)
        .unwrap();
    let metadata = BranchMetadata::from(&record);
    let (mut backend_transport, server_transport) = duplex();
    let subscriber = target.accept_subscriber_with_trust(
        server_transport,
        backend_identity,
        CommitUnitTrust::TrustedBackend,
    );

    backend_transport
        .send(SyncMessage::BranchMetadata(metadata.clone()))
        .unwrap();
    subscriber.borrow_mut().tick().unwrap();
    assert_eq!(target.node().borrow().branch_record(branch), Some(&record));

    backend_transport
        .send(SyncMessage::BranchMetadata(metadata))
        .unwrap();
    subscriber.borrow_mut().tick().unwrap();
    assert_eq!(target.node().borrow().branch_record(branch), Some(&record));
}

#[test]
fn trusted_backend_discards_branch_metadata_once_and_recovers_it() {
    // Internal trust/storage boundary test: lifecycle metadata is carried only
    // by trusted backend links and must be durable before branch data is routed.
    let schema = schema();
    let backend_identity = AuthorId::from_bytes([0xb0; 16]);
    let node_uuid = NodeUuid::from_bytes([0x5e; 16]);
    let branch = BranchId::from_bytes([0x46; 16]);
    let source = open_core(0x5e, AuthorId::SYSTEM, &schema);
    let open_record = source
        .node()
        .borrow_mut()
        .create_branch_as(branch, backend_identity)
        .unwrap();
    let open_metadata = BranchMetadata::from(&open_record);
    let mut discarded_metadata = open_metadata.clone();
    discarded_metadata.open = false;
    let dir = tempfile::tempdir().unwrap();
    let cfs = schema.column_families();
    let refs = cfs.iter().map(String::as_str).collect::<Vec<_>>();
    let storage = RocksDbStorage::open(dir.path(), &refs).unwrap();
    let target =
        Node::new(NodeState::new_history_complete(node_uuid, schema.clone(), storage).unwrap());
    let (mut backend_transport, server_transport) = duplex();
    let subscriber = target.accept_subscriber_with_trust(
        server_transport,
        backend_identity,
        CommitUnitTrust::TrustedBackend,
    );

    backend_transport
        .send(SyncMessage::BranchMetadata(open_metadata.clone()))
        .unwrap();
    subscriber.borrow_mut().tick().unwrap();
    backend_transport
        .send(SyncMessage::BranchMetadata(discarded_metadata.clone()))
        .unwrap();
    subscriber.borrow_mut().tick().unwrap();
    backend_transport
        .send(SyncMessage::BranchMetadata(discarded_metadata.clone()))
        .unwrap();
    subscriber.borrow_mut().tick().unwrap();
    let discarded_record = target
        .node()
        .borrow()
        .branch_record(branch)
        .cloned()
        .unwrap();
    assert_eq!(discarded_record.created_by, open_record.created_by);
    assert_eq!(discarded_record.parent, open_record.parent);
    assert_eq!(discarded_record.base, open_record.base);
    assert!(!BranchMetadata::from(&discarded_record).open);

    drop(subscriber);
    drop(target);
    let storage = RocksDbStorage::open(dir.path(), &refs).unwrap();
    let reopened = Node::new(NodeState::new_history_complete(node_uuid, schema, storage).unwrap());
    assert_eq!(
        reopened.node().borrow().branch_record(branch),
        Some(&discarded_record)
    );

    let (mut reverse_transport, server_transport) = duplex();
    let reverse = reopened.accept_subscriber_with_trust(
        server_transport,
        backend_identity,
        CommitUnitTrust::TrustedBackend,
    );
    reverse_transport
        .send(SyncMessage::BranchMetadata(open_metadata))
        .unwrap();
    assert!(reverse.borrow_mut().tick().is_err());

    let mut changed_creator = discarded_metadata.clone();
    changed_creator.created_by = AuthorId::from_bytes([0xee; 16]);
    let mut changed_parent = discarded_metadata.clone();
    changed_parent.parent = Some(BranchId::from_bytes([0xdd; 16]));
    let mut changed_base = discarded_metadata;
    changed_base.base = None;
    for mutation in [changed_creator, changed_parent, changed_base] {
        let (mut mutation_transport, server_transport) = duplex();
        let mutation_connection = reopened.accept_subscriber_with_trust(
            server_transport,
            backend_identity,
            CommitUnitTrust::TrustedBackend,
        );
        mutation_transport
            .send(SyncMessage::BranchMetadata(mutation))
            .unwrap();
        assert!(mutation_connection.borrow_mut().tick().is_err());
    }
    assert_eq!(
        reopened.node().borrow().branch_record(branch),
        Some(&discarded_record)
    );
}

#[test]
fn subscriber_connection_serves_single_branch_read_view_subscription() {
    let schema = schema();
    let client_author = AuthorId::from_bytes([0xc1; 16]);
    let server = open_core(0x5e, AuthorId::SYSTEM, &schema);
    let branch = BranchId(uuid::Uuid::from_bytes([0x42; 16]));
    server
        .node()
        .borrow_mut()
        .create_branch(branch)
        .expect("create branch");
    server
        .node()
        .borrow_mut()
        .commit_mergeable_on_branch(
            branch,
            MergeableCommit::new("todos", row(0x42), 10).cells(cells(
                "branch-only",
                false,
                client_author,
            )),
        )
        .expect("commit branch row");

    let (mut client_transport, server_transport) = duplex();
    let subscriber = server.accept_subscriber(server_transport, client_author);
    let shape = Query::from("todos").validate(&schema).unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    let read_opts = branch_read_opts();
    let opts = RegisterShapeOptions {
        tier: DurabilityTier::Global,
        read_view: read_opts.read_view,
    };
    let subscription = SubscriptionKey {
        shape_id: shape.shape_id(),
        binding_id: binding.binding_id(),
        read_view: opts.read_view_key(),
    };

    client_transport
        .send(SyncMessage::RegisterShape {
            shape_id: shape.shape_id(),
            ast: ShapeAst::from_validated(&shape),
            opts: opts.clone(),
        })
        .unwrap();
    client_transport
        .send(SyncMessage::Subscribe(Subscribe {
            shape_id: shape.shape_id(),
            subscription,
            values: Vec::new(),
            known_state: None,
        }))
        .unwrap();

    subscriber.borrow_mut().tick().unwrap();
    assert_subscribe_rejected_branch_overlay(
        try_recv_subscriber_payload(client_transport.as_mut())
            .expect("expected subscription rejection"),
        subscription,
    );
    subscriber.borrow_mut().tick().unwrap();
    client_transport
        .send(SyncMessage::Unsubscribe { subscription })
        .unwrap();
    subscriber.borrow_mut().tick().unwrap();
}

#[test]
fn subscriber_connection_rejects_one_gapped_subscription_and_keeps_serving_others() {
    let schema = schema();
    let owner = AuthorId::from_bytes([0xa1; 16]);
    let client_author = AuthorId::from_bytes([0xc1; 16]);
    let server = open_core(0x5e, AuthorId::SYSTEM, &schema);
    let branch = BranchId(uuid::Uuid::from_bytes([0x42; 16]));
    server
        .node()
        .borrow_mut()
        .create_branch(branch)
        .expect("create branch");
    seed(&server, "todos", cells("first", false, owner));

    let (mut client_transport, server_transport) = duplex();
    let subscriber = server.accept_subscriber(server_transport, client_author);
    let shape = Query::from("todos").validate(&schema).unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    let supported_subscription = SubscriptionKey {
        shape_id: shape.shape_id(),
        binding_id: binding.binding_id(),
        read_view: RegisterShapeOptions::default().read_view_key(),
    };
    let branch_opts = RegisterShapeOptions {
        tier: DurabilityTier::Global,
        read_view: branch_read_opts().read_view,
    };
    let branch_subscription = SubscriptionKey {
        shape_id: shape.shape_id(),
        binding_id: binding.binding_id(),
        read_view: branch_opts.read_view_key(),
    };

    client_transport
        .send(SyncMessage::RegisterShape {
            shape_id: shape.shape_id(),
            ast: ShapeAst::from_validated(&shape),
            opts: RegisterShapeOptions::default(),
        })
        .unwrap();
    client_transport
        .send(SyncMessage::Subscribe(Subscribe {
            shape_id: shape.shape_id(),
            subscription: supported_subscription,
            values: Vec::new(),
            known_state: None,
        }))
        .unwrap();
    subscriber.borrow_mut().tick().unwrap();
    assert_view_update_for_subscription(
        try_recv_subscriber_payload(client_transport.as_mut())
            .expect("expected initial supported view update"),
        supported_subscription,
    );

    client_transport
        .send(SyncMessage::RegisterShape {
            shape_id: shape.shape_id(),
            ast: ShapeAst::from_validated(&shape),
            opts: branch_opts,
        })
        .unwrap();
    client_transport
        .send(SyncMessage::Subscribe(Subscribe {
            shape_id: shape.shape_id(),
            subscription: branch_subscription,
            values: Vec::new(),
            known_state: None,
        }))
        .unwrap();
    subscriber.borrow_mut().tick().unwrap();
    assert_subscribe_rejected_branch_overlay(
        try_recv_subscriber_payload(client_transport.as_mut())
            .expect("expected branch subscription rejection"),
        branch_subscription,
    );
    subscriber.borrow_mut().tick().unwrap();

    seed(&server, "todos", cells("second", false, owner));
    subscriber.borrow_mut().tick().unwrap();
    assert_view_update_for_subscription(
        try_recv_subscriber_payload(client_transport.as_mut())
            .expect("expected supported update after rejection"),
        supported_subscription,
    );
}

#[test]
fn db_subscription_stream_surfaces_upstream_rejection_after_open() {
    let schema = schema();
    let owner = AuthorId::from_bytes([0xa1; 16]);
    let db = open_db(0x51, owner, &schema);
    let (client_transport, mut server_transport) = duplex();
    let upstream = db.connect_upstream(client_transport);

    let prepared = db.prepare_query(&Query::from("todos")).unwrap();
    let mut subscription = block_on(db.subscribe(&prepared, ReadOpts::default()))
        .expect("local subscription should open before upstream response");
    assert!(matches!(
        block_on(subscription.next_event()),
        Some(SubscriptionEvent::Delta { reset: true, .. })
    ));

    upstream.borrow_mut().tick().unwrap();
    let mut subscribed = None;
    while let Some(message) = server_transport.try_recv() {
        if let SyncMessage::Subscribe(subscribe) = message {
            subscribed = Some(subscribe.subscription);
        }
    }
    let subscribed = subscribed.expect("expected upstream subscribe command");

    server_transport
        .send(SyncMessage::SubscribeRejected {
            subscription: subscribed,
            reason: SubscribeRejectReason::UnsupportedShapeCapability {
                detail: "server does not support this maintained shape".to_owned(),
            },
        })
        .unwrap();
    upstream.borrow_mut().tick().unwrap();

    match block_on(subscription.next_event()) {
        Some(SubscriptionEvent::Rejected {
            reason: SubscribeRejectReason::UnsupportedShapeCapability { detail },
        }) => assert_eq!(detail, "server does not support this maintained shape"),
        other => panic!("expected stream-carried rejection, got {other:?}"),
    }
}

#[test]
fn upstream_transport_rejects_forged_system_catalogue_publication() {
    let base = schema();
    let client_author = AuthorId::from_bytes([0x51; 16]);
    let client = open_db(0x51, client_author, &base);
    let (client_transport, mut upstream_transport) = duplex();
    let upstream = client.connect_upstream(client_transport);
    let target = SchemaVersion::new(JazzSchema::new([TableSchema::new(
        "todos",
        [
            ColumnSchema::new("title", ColumnType::String),
            ColumnSchema::new("done", ColumnType::Bool),
            ColumnSchema::new("owner", ColumnType::Uuid),
            ColumnSchema::new("body", ColumnType::String),
        ],
    )
    .with_read_policy(Policy::public())
    .with_write_policy(Policy::public())]));
    let lens = MigrationLens::new(
        base.version_id(),
        target.id,
        vec![TableLens {
            source_table: "todos".to_owned(),
            target_table: "todos".to_owned(),
            ops: vec![LensOp::AddColumn {
                column: "body".to_owned(),
                default: Value::String(String::new()),
            }],
        }],
    );
    upstream_transport
        .send(SyncMessage::PublishSchemaWithLens {
            author: AuthorId::SYSTEM,
            catalogue_seq: 1,
            publication: Box::new(SchemaLineagePublication::new(
                target.clone(),
                lens,
                Vec::<String>::new(),
                Vec::<String>::new(),
            )),
        })
        .unwrap();

    let error = upstream.borrow_mut().tick().unwrap_err();
    assert_eq!(error.code, ErrorCode::Protocol);
    assert!(error.message.contains("unauthorized catalogue update"));
    assert!(client.catalogue_schema(target.id).is_none());
}

#[test]
fn subscriber_connection_surfaces_server_table_not_found_without_silence() {
    let server_schema = schema();
    let owner = AuthorId::from_bytes([0xa1; 16]);
    let server = open_core(0x53, AuthorId::SYSTEM, &server_schema);
    let (mut client_transport, server_transport) = duplex();
    let subscriber = server.accept_subscriber(server_transport, owner);
    let shape_id = ShapeId(uuid::Uuid::from_bytes([0x52; 16]));
    let subscription = SubscriptionKey {
        shape_id,
        binding_id: BindingId(uuid::Uuid::nil()),
        read_view: RegisterShapeOptions::default().read_view_key(),
    };

    // This must exercise the wire boundary because public query preparation
    // correctly refuses an unknown table before it can be sent. Previously the
    // server dropped this registration, so the following Subscribe would have
    // waited forever; the public stream routing is covered separately above.
    client_transport
        .send(SyncMessage::RegisterShape {
            shape_id,
            ast: ShapeAst::new(Query::from("people"), server_schema.version_id()),
            opts: RegisterShapeOptions::default(),
        })
        .unwrap();
    client_transport
        .send(SyncMessage::Subscribe(Subscribe {
            shape_id,
            subscription,
            values: Vec::new(),
            known_state: None,
        }))
        .unwrap();

    subscriber.borrow_mut().tick().unwrap();

    match try_recv_subscriber_payload(client_transport.as_mut()) {
        Some(SyncMessage::SubscribeRejected {
            subscription: rejected_subscription,
            reason:
                SubscribeRejectReason::ServerFailure {
                    code: SubscribeServerFailureCode::TableNotFound,
                },
        }) => assert_eq!(rejected_subscription, subscription),
        other => panic!("expected table-not-found rejection, got {other:?}"),
    }
}

#[test]
fn subscriber_connection_serves_default_ordered_window_alongside_unbounded_shape() {
    let schema = schema();
    let owner = AuthorId::from_bytes([0xa1; 16]);
    let client_author = AuthorId::from_bytes([0xc1; 16]);
    let server = open_core(0x5e, AuthorId::SYSTEM, &schema);
    seed(&server, "todos", cells("first", false, owner));
    seed(&server, "todos", cells("second", false, owner));

    // Protocol-level coverage for the current prepared/policy routing path:
    // keep an ordinary root and a default-ordered offset window live together.
    let (mut client_transport, server_transport) = duplex();
    let subscriber = server.accept_subscriber(server_transport, client_author);
    let supported_shape = Query::from("todos").validate(&schema).unwrap();
    let window_shape = Query::from("todos")
        .offset(1)
        .limit(1)
        .validate(&schema)
        .unwrap();
    let supported_binding = supported_shape.bind(BTreeMap::new()).unwrap();
    let window_binding = window_shape.bind(BTreeMap::new()).unwrap();
    let supported_subscription = SubscriptionKey {
        shape_id: supported_shape.shape_id(),
        binding_id: supported_binding.binding_id(),
        read_view: RegisterShapeOptions::default().read_view_key(),
    };
    let window_subscription = SubscriptionKey {
        shape_id: window_shape.shape_id(),
        binding_id: window_binding.binding_id(),
        read_view: RegisterShapeOptions::default().read_view_key(),
    };

    client_transport
        .send(SyncMessage::RegisterShape {
            shape_id: window_shape.shape_id(),
            ast: ShapeAst::from_validated(&window_shape),
            opts: RegisterShapeOptions::default(),
        })
        .unwrap();
    client_transport
        .send(SyncMessage::RegisterShape {
            shape_id: supported_shape.shape_id(),
            ast: ShapeAst::from_validated(&supported_shape),
            opts: RegisterShapeOptions::default(),
        })
        .unwrap();
    client_transport
        .send(SyncMessage::Subscribe(Subscribe {
            shape_id: supported_shape.shape_id(),
            subscription: supported_subscription,
            values: Vec::new(),
            known_state: None,
        }))
        .unwrap();

    subscriber.borrow_mut().tick().unwrap();
    assert_view_update_for_subscription(
        try_recv_subscriber_payload(client_transport.as_mut())
            .expect("expected unbounded subscription update"),
        supported_subscription,
    );

    client_transport
        .send(SyncMessage::Subscribe(Subscribe {
            shape_id: window_shape.shape_id(),
            subscription: window_subscription,
            values: Vec::new(),
            known_state: None,
        }))
        .unwrap();
    seed(&server, "todos", cells("third", false, owner));
    subscriber.borrow_mut().tick().unwrap();
    let first = try_recv_subscriber_payload(client_transport.as_mut())
        .expect("expected maintained subscription update");
    let second = try_recv_subscriber_payload(client_transport.as_mut())
        .expect("expected maintained window update");
    let subscriptions = [first, second]
        .into_iter()
        .map(|message| match message {
            SyncMessage::ViewUpdate { subscription, .. } => subscription,
            other => panic!("expected ViewUpdate, got {other:?}"),
        })
        .collect::<BTreeSet<_>>();
    assert_eq!(
        subscriptions,
        BTreeSet::from([supported_subscription, window_subscription]),
        "both the unbounded and default-ordered window subscriptions remain served"
    );
}

#[test]
fn subscriber_connection_rejects_local_tier_register_shape() {
    let schema = schema();
    let owner = AuthorId::from_bytes([0xa1; 16]);
    let client_author = AuthorId::from_bytes([0xc1; 16]);
    let server = open_core(0x5e, AuthorId::SYSTEM, &schema);
    seed(&server, "todos", cells("after malformed", false, owner));

    // Internal sync-loop coverage: public propagated subscriptions normalize
    // local reads before sending RegisterShape, so this sends protocol messages
    // directly to exercise the lower serving fence.
    let (mut client_transport, server_transport) = duplex();
    let subscriber = server.accept_subscriber(server_transport, client_author);
    let shape = Query::from("todos").validate(&schema).unwrap();
    let opts = RegisterShapeOptions {
        tier: DurabilityTier::Local,
        read_view: ReadViewSpec::default(),
    };
    let rejected_read_view = opts.read_view_key();

    client_transport
        .send(SyncMessage::RegisterShape {
            shape_id: shape.shape_id(),
            ast: ShapeAst::from_validated(&shape),
            opts,
        })
        .unwrap();

    subscriber.borrow_mut().tick().unwrap();
    assert_subscribe_rejected_unsupported_shape_capability_detail(
        try_recv_subscriber_payload(client_transport.as_mut())
            .expect("expected local-tier registration rejection"),
        SubscriptionKey {
            shape_id: shape.shape_id(),
            binding_id: BindingId(uuid::Uuid::nil()),
            read_view: rejected_read_view,
        },
        "global-tier registration",
    );

    let binding = shape.bind(BTreeMap::new()).unwrap();
    let subscription = SubscriptionKey {
        shape_id: shape.shape_id(),
        binding_id: binding.binding_id(),
        read_view: RegisterShapeOptions::default().read_view_key(),
    };
    client_transport
        .send(SyncMessage::RegisterShape {
            shape_id: shape.shape_id(),
            ast: ShapeAst::from_validated(&shape),
            opts: RegisterShapeOptions::default(),
        })
        .unwrap();
    client_transport
        .send(SyncMessage::Subscribe(Subscribe {
            shape_id: shape.shape_id(),
            subscription,
            values: Vec::new(),
            known_state: None,
        }))
        .unwrap();

    subscriber.borrow_mut().tick().unwrap();
    assert_view_update_for_subscription(
        try_recv_subscriber_payload(client_transport.as_mut())
            .expect("valid subscription should still be served after malformed register"),
        subscription,
    );
}

#[test]
fn subscriber_connection_rejects_subscribe_without_link_shape_options() {
    let schema = schema();
    let client_author = AuthorId::from_bytes([0xc1; 16]);
    let server = open_core(0x5e, AuthorId::SYSTEM, &schema);

    // Internal sync-loop coverage: pre-register the shape in the served node but
    // not on this link. The subscriber must still RegisterShape on its own
    // connection so serving options cannot leak across links.
    let (mut client_transport, server_transport) = duplex();
    let subscriber = server.accept_subscriber(server_transport, client_author);
    let shape = Query::from("todos").validate(&schema).unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    server
        .node()
        .borrow_mut()
        .apply_sync_message(SyncMessage::RegisterShape {
            shape_id: shape.shape_id(),
            ast: ShapeAst::from_validated(&shape),
            opts: RegisterShapeOptions::default(),
        })
        .unwrap();

    client_transport
        .send(SyncMessage::Subscribe(Subscribe {
            shape_id: shape.shape_id(),
            subscription: SubscriptionKey {
                shape_id: shape.shape_id(),
                binding_id: binding.binding_id(),
                read_view: RegisterShapeOptions::default().read_view_key(),
            },
            values: Vec::new(),
            known_state: None,
        }))
        .unwrap();

    subscriber.borrow_mut().tick().unwrap();
    assert_eq!(
        server
            .node()
            .borrow()
            .sync_metrics()
            .dropped_peer_request_messages,
        1
    );
}

#[test]
fn subscriber_connection_drops_oversized_known_state_and_keeps_serving() {
    let schema = schema();
    let owner = AuthorId::from_bytes([0xa1; 16]);
    let client_author = AuthorId::from_bytes([0xc1; 16]);
    let server = open_core(0x5e, AuthorId::SYSTEM, &schema);
    seed(&server, "todos", cells("after malformed", false, owner));

    let (mut client_transport, server_transport) = duplex();
    let subscriber = server.accept_subscriber(server_transport, client_author);
    let shape = Query::from("todos").validate(&schema).unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    let subscription = SubscriptionKey {
        shape_id: shape.shape_id(),
        binding_id: binding.binding_id(),
        read_view: RegisterShapeOptions::default().read_view_key(),
    };

    client_transport
        .send(SyncMessage::RegisterShape {
            shape_id: shape.shape_id(),
            ast: ShapeAst::from_validated(&shape),
            opts: RegisterShapeOptions::default(),
        })
        .unwrap();
    client_transport
        .send(SyncMessage::Subscribe(Subscribe {
            shape_id: shape.shape_id(),
            subscription,
            values: Vec::new(),
            known_state: Some(KnownStateDeclaration::ExactVersionSet {
                versions: oversized_row_version_refs(MAX_KNOWN_STATE_EXACT_REFS + 1),
            }),
        }))
        .unwrap();

    subscriber.borrow_mut().tick().unwrap();
    assert_eq!(
        server
            .node()
            .borrow()
            .sync_metrics()
            .dropped_peer_request_messages,
        1
    );
    assert!(
        try_recv_subscriber_payload(client_transport.as_mut()).is_none(),
        "oversized known-state request should not receive a view update"
    );

    client_transport
        .send(SyncMessage::Subscribe(Subscribe {
            shape_id: shape.shape_id(),
            subscription,
            values: Vec::new(),
            known_state: Some(KnownStateDeclaration::Fast {
                completeness: KnownStateCompleteness::FastCurrentMembership,
                position: crate::time::GlobalSeq::default(),
            }),
        }))
        .unwrap();

    subscriber.borrow_mut().tick().unwrap();
    assert_view_update_for_subscription(
        try_recv_subscriber_payload(client_transport.as_mut())
            .expect("valid resubscribe should be served after malformed known-state"),
        subscription,
    );
}

#[test]
fn subscriber_connection_drops_oversized_fetch_row_versions_and_keeps_serving() {
    let schema = schema();
    let owner = AuthorId::from_bytes([0xa1; 16]);
    let client_author = AuthorId::from_bytes([0xc1; 16]);
    let server = open_core(0x5e, AuthorId::SYSTEM, &schema);
    seed(&server, "todos", cells("after malformed", false, owner));

    let (mut client_transport, server_transport) = duplex();
    let subscriber = server.accept_subscriber(server_transport, client_author);
    let shape = Query::from("todos").validate(&schema).unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    let subscription = SubscriptionKey {
        shape_id: shape.shape_id(),
        binding_id: binding.binding_id(),
        read_view: RegisterShapeOptions::default().read_view_key(),
    };

    client_transport
        .send(SyncMessage::FetchRowVersions {
            requests: oversized_row_version_refs(MAX_FETCH_ROW_VERSIONS + 1),
        })
        .unwrap();
    subscriber.borrow_mut().tick().unwrap();
    assert_eq!(
        server
            .node()
            .borrow()
            .sync_metrics()
            .dropped_peer_request_messages,
        1
    );

    client_transport
        .send(SyncMessage::RegisterShape {
            shape_id: shape.shape_id(),
            ast: ShapeAst::from_validated(&shape),
            opts: RegisterShapeOptions::default(),
        })
        .unwrap();
    client_transport
        .send(SyncMessage::Subscribe(Subscribe {
            shape_id: shape.shape_id(),
            subscription,
            values: Vec::new(),
            known_state: None,
        }))
        .unwrap();

    subscriber.borrow_mut().tick().unwrap();
    assert_view_update_for_subscription(
        try_recv_subscriber_payload(client_transport.as_mut())
            .expect("valid subscription should still be served after malformed repair request"),
        subscription,
    );
}

#[test]
fn subscriber_connection_drops_mismatched_shape_id_and_keeps_serving() {
    let schema = schema();
    let owner = AuthorId::from_bytes([0xa1; 16]);
    let client_author = AuthorId::from_bytes([0xc1; 16]);
    let server = open_core(0x5e, AuthorId::SYSTEM, &schema);
    seed(&server, "todos", cells("after malformed", false, owner));

    let (mut client_transport, server_transport) = duplex();
    let subscriber = server.accept_subscriber(server_transport, client_author);
    let shape = Query::from("todos").validate(&schema).unwrap();
    let other_shape = Query::from("todos")
        .filter(eq(col("done"), lit(true)))
        .validate(&schema)
        .unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    let subscription = SubscriptionKey {
        shape_id: shape.shape_id(),
        binding_id: binding.binding_id(),
        read_view: RegisterShapeOptions::default().read_view_key(),
    };

    client_transport
        .send(SyncMessage::RegisterShape {
            shape_id: other_shape.shape_id(),
            ast: ShapeAst::from_validated(&shape),
            opts: RegisterShapeOptions::default(),
        })
        .unwrap();
    subscriber.borrow_mut().tick().unwrap();
    assert_eq!(
        server
            .node()
            .borrow()
            .sync_metrics()
            .dropped_peer_request_messages,
        1
    );

    client_transport
        .send(SyncMessage::RegisterShape {
            shape_id: shape.shape_id(),
            ast: ShapeAst::from_validated(&shape),
            opts: RegisterShapeOptions::default(),
        })
        .unwrap();
    client_transport
        .send(SyncMessage::Subscribe(Subscribe {
            shape_id: shape.shape_id(),
            subscription,
            values: Vec::new(),
            known_state: None,
        }))
        .unwrap();

    subscriber.borrow_mut().tick().unwrap();
    assert_view_update_for_subscription(
        try_recv_subscriber_payload(client_transport.as_mut())
            .expect("valid subscription should still be served after mismatched shape id"),
        subscription,
    );
}

#[test]
fn local_live_subscription_requests_global_upstream_coverage() {
    let schema = schema();
    let owner = AuthorId::from_bytes([0xa1; 16]);
    let client_author = AuthorId::from_bytes([0xc1; 16]);

    let server = open_core(0x5e, AuthorId::SYSTEM, &schema);
    let client = open_db(0xc1, client_author, &schema);
    seed(&server, "todos", cells("first", false, owner));

    let (client_transport, server_transport) = duplex();
    let _upstream = client.connect_upstream(client_transport);
    let subscriber = server.accept_subscriber(server_transport, client_author);

    let query = Query::from("todos");
    let mut subscription = prepared_subscribe(&client, &query, ReadOpts::default()).unwrap();
    assert!(opened_rows(block_on(subscription.next_event()).unwrap()).is_empty());
    client.tick().unwrap();
    server.tick().unwrap();

    // Internal sync-loop coverage: the public subscription is local-tier, but
    // the remote coverage request must be settled-only because local state is
    // link-local to the subscribing client.
    let subscriber_ref = subscriber.borrow();
    let ConnectionLink::Subscriber {
        coverage_groups, ..
    } = &subscriber_ref.link
    else {
        panic!("expected subscriber connection");
    };
    assert_eq!(coverage_groups.len(), 1);
    let coverage = coverage_groups.keys().next().unwrap();
    assert_eq!(coverage.opts.tier, DurabilityTier::Global);
    assert!(coverage.opts.read_view.is_default());
}

#[test]
fn edge_live_subscription_requests_global_upstream_coverage() {
    let schema = schema();
    let client_author = AuthorId::from_bytes([0xc1; 16]);
    let server = open_core(0x5e, AuthorId::SYSTEM, &schema);
    let client = open_db(0xc1, client_author, &schema);
    let (client_transport, server_transport) = duplex();
    let _upstream = client.connect_upstream(client_transport);
    let subscriber = server.accept_subscriber(server_transport, client_author);

    let query = Query::from("todos");
    let mut subscription = prepared_subscribe(&client, &query, edge_subscribe_opts()).unwrap();
    assert!(opened_rows(block_on(subscription.next_event()).unwrap()).is_empty());

    client.tick().unwrap();
    server.tick().unwrap();

    // Edge-tier is the local visible tier for browser clients, but propagated
    // upstream coverage is still registered at global tier. Edge serving is
    // link-local; the subscription's settled contract is satisfied when the
    // globally settled coverage arrives back at the client.
    let subscriber_ref = subscriber.borrow();
    let ConnectionLink::Subscriber {
        coverage_groups, ..
    } = &subscriber_ref.link
    else {
        panic!("expected subscriber connection");
    };
    assert_eq!(coverage_groups.len(), 1);
    let coverage = coverage_groups.keys().next().unwrap();
    assert_eq!(coverage.opts.tier, DurabilityTier::Global);
    assert!(coverage.opts.read_view.is_default());
}

#[test]
fn subscriber_connection_rejects_non_global_register_shape_options() {
    let schema = schema();
    let client_author = AuthorId::from_bytes([0xc1; 16]);
    let server = open_core(0x5e, AuthorId::SYSTEM, &schema);

    // Internal sync-loop coverage: public APIs normalize local subscriptions to
    // global upstream coverage. Malformed/direct peers must not install an
    // unsupported edge-tier subscription.
    let (mut client_transport, server_transport) = duplex();
    let subscriber = server.accept_subscriber(server_transport, client_author);
    let shape = Query::from("todos").validate(&schema).unwrap();
    let edge_opts = RegisterShapeOptions {
        tier: DurabilityTier::Edge,
        read_view: ReadViewSpec::default(),
    };
    let rejected_read_view = edge_opts.read_view_key();

    client_transport
        .send(SyncMessage::RegisterShape {
            shape_id: shape.shape_id(),
            ast: ShapeAst::from_validated(&shape),
            opts: edge_opts,
        })
        .unwrap();

    subscriber.borrow_mut().tick().unwrap();
    assert_subscribe_rejected_unsupported_shape_capability_detail(
        try_recv_subscriber_payload(client_transport.as_mut())
            .expect("expected edge-tier registration rejection"),
        SubscriptionKey {
            shape_id: shape.shape_id(),
            binding_id: BindingId(uuid::Uuid::nil()),
            read_view: rejected_read_view,
        },
        "global-tier registration",
    );
}

#[test]
fn subscriber_connection_accepts_array_subquery_register_shape_for_serving_subscription() {
    let schema = relation_schema();
    let client_author = AuthorId::from_bytes([0xc1; 16]);
    let server = open_core(0x5e, AuthorId::SYSTEM, &schema);

    // Internal sync-loop coverage: array-subquery subscriptions are served as
    // flat relation-edge facts, so direct wire registration should be accepted.
    let (mut client_transport, server_transport) = duplex();
    let subscriber = server.accept_subscriber(server_transport, client_author);
    let shape = Query::from("users")
        .array_subquery(ArraySubquery::new("todos", "todos", "owner_id", "id"))
        .validate(&schema)
        .unwrap();

    client_transport
        .send(SyncMessage::RegisterShape {
            shape_id: shape.shape_id(),
            ast: ShapeAst::from_validated(&shape),
            opts: RegisterShapeOptions::default(),
        })
        .unwrap();

    subscriber.borrow_mut().tick().unwrap();
    assert!(
        try_recv_subscriber_payload(client_transport.as_mut()).is_none(),
        "registering a supported array-subquery shape should not emit a rejection"
    );
}

#[test]
fn subscriber_connection_accepts_relation_register_shape_for_serving_subscription() {
    let schema = relation_schema();
    let client_author = AuthorId::from_bytes([0xc1; 16]);
    let server = open_core(0x5e, AuthorId::SYSTEM, &schema);
    server
        .insert_with_id(
            "users",
            row(0xa1),
            BTreeMap::from([("name".to_owned(), Value::String("alice".to_owned()))]),
        )
        .unwrap();
    server
        .insert_with_id(
            "todos",
            row(0x11),
            BTreeMap::from([
                ("title".to_owned(), Value::String("alice todo".to_owned())),
                ("owner_id".to_owned(), Value::Uuid(row(0xa1).0)),
            ]),
        )
        .unwrap();
    let (mut client_transport, server_transport) = duplex();
    let subscriber = server.accept_subscriber(server_transport, client_author);

    let relation = RelationQuery {
        rel: RelationExpr::Project {
            input: Box::new(RelationExpr::Join {
                left: Box::new(RelationExpr::TableScan {
                    table: "users".to_owned(),
                    alias: None,
                }),
                right: Box::new(RelationExpr::TableScan {
                    table: "todos".to_owned(),
                    alias: Some("__hop_0".to_owned()),
                }),
                on: vec![crate::query::RelationJoinCondition {
                    left: RelationColumnRef {
                        scope: Some("users".to_owned()),
                        column: "id".to_owned(),
                    },
                    right: RelationColumnRef {
                        scope: Some("__hop_0".to_owned()),
                        column: "owner_id".to_owned(),
                    },
                }],
                join_kind: RelationJoinKind::Inner,
            }),
            columns: vec![
                crate::query::RelationProjectColumn {
                    alias: "id".to_owned(),
                    expr: RelationProjectExpr::RowId(RelationRowIdRef::Current),
                },
                crate::query::RelationProjectColumn {
                    alias: "title".to_owned(),
                    expr: RelationProjectExpr::Column(RelationColumnRef {
                        scope: Some("__hop_0".to_owned()),
                        column: "title".to_owned(),
                    }),
                },
                crate::query::RelationProjectColumn {
                    alias: "owner_id".to_owned(),
                    expr: RelationProjectExpr::Column(RelationColumnRef {
                        scope: Some("__hop_0".to_owned()),
                        column: "owner_id".to_owned(),
                    }),
                },
            ],
        },
    };
    let normalized = relation_query_to_query(&relation)
        .unwrap()
        .validate(&schema)
        .unwrap();
    let binding = normalized.bind(BTreeMap::new()).unwrap();
    let subscription = SubscriptionKey {
        shape_id: normalized.shape_id(),
        binding_id: binding.binding_id(),
        read_view: RegisterShapeOptions::default().read_view_key(),
    };

    client_transport
        .send(SyncMessage::RegisterShape {
            shape_id: normalized.shape_id(),
            ast: ShapeAst::new_relation(relation, schema.version_id()),
            opts: RegisterShapeOptions::default(),
        })
        .unwrap();
    client_transport
        .send(SyncMessage::Subscribe(Subscribe {
            shape_id: normalized.shape_id(),
            subscription,
            values: Vec::new(),
            known_state: None,
        }))
        .unwrap();

    subscriber.borrow_mut().tick().unwrap();
    let Some(SyncMessage::ViewUpdate {
        subscription: served,
        result_member_adds,
        ..
    }) = try_recv_subscriber_payload(client_transport.as_mut())
    else {
        panic!("expected relation facade subscription view update");
    };
    assert_eq!(served, subscription);
    assert!(
        result_member_adds.iter().any(|member| {
            let Some(member) = member.as_real_row() else {
                return false;
            };
            member.table.as_str() == "todos" && member.row_uuid == row(0x11)
        }),
        "relation facade subscription should deliver the projected target row"
    );
}

#[test]
fn subscription_emits_when_remote_coverage_settles_without_row_changes() {
    let schema = schema();
    let client_author = AuthorId::from_bytes([0xc1; 16]);

    let server = open_core(0x5e, AuthorId::SYSTEM, &schema);
    let client = open_db(0xc1, client_author, &schema);

    let (client_transport, server_transport) = duplex();
    let _upstream = client.connect_upstream(client_transport);
    let _subscriber = server.accept_subscriber(server_transport, client_author);

    let query = Query::from("todos");
    let mut subscription = prepared_subscribe(&client, &query, global_subscribe_opts()).unwrap();
    let opened = block_on(subscription.next_event()).unwrap();
    assert!(!event_settled(&opened));
    assert!(opened_rows(opened).is_empty());

    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();

    let settled = block_on(subscription.next_event()).unwrap();
    assert!(event_settled(&settled));
    let (added, updated, removed) = delta_rows(settled);
    assert!(added.is_empty());
    assert!(updated.is_empty());
    assert!(removed.is_empty());
}

#[test]
fn one_shot_propagated_query_records_empty_remote_coverage() {
    let schema = schema();
    let client_author = AuthorId::from_bytes([0xc1; 16]);

    let server = open_core(0x5e, AuthorId::SYSTEM, &schema);
    let client = open_db(0xc1, client_author, &schema);

    let (client_transport, server_transport) = duplex();
    let _upstream = client.connect_upstream(client_transport);
    let _subscriber = server.accept_subscriber(server_transport, client_author);

    let query = Query::from("todos");
    let prepared = prepared(&client, &query);

    let attachment = client
        .attach_query_with_opts(&prepared, global_subscribe_opts())
        .unwrap();
    assert!(!client.query_attachment_is_covered(&attachment));
    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();

    assert!(client.query_attachment_is_covered(&attachment));
    assert!(prepared_read(&client, &query).is_empty());
    client.detach_query(attachment);
}

#[test]
fn one_shot_propagated_query_attaches_fresh_usage_subscription_for_covered_binding() {
    let schema = schema();
    let owner = AuthorId::from_bytes([0xa1; 16]);
    let client_author = AuthorId::from_bytes([0xc1; 16]);

    let server = open_core(0x5e, AuthorId::SYSTEM, &schema);
    let client = open_db(0xc1, client_author, &schema);

    seed(&server, "todos", cells("first", false, owner));

    let (client_transport, server_transport) = duplex();
    let _upstream = client.connect_upstream(client_transport);
    let _subscriber = server.accept_subscriber(server_transport, client_author);

    let query = Query::from("todos");
    let prepared = prepared(&client, &query);
    let first_attachment = client
        .attach_query_with_opts(&prepared, global_subscribe_opts())
        .unwrap();
    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();
    assert!(client.query_attachment_is_covered(&first_attachment));
    assert_eq!(prepared_read(&client, &query).len(), 1);

    seed(&server, "todos", cells("second", false, owner));
    let second_attachment = client
        .attach_query_with_opts(&prepared, global_subscribe_opts())
        .unwrap();
    assert!(client.query_attachment_is_covered(&first_attachment));
    assert!(!client.query_attachment_is_covered(&second_attachment));
    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();

    assert!(client.query_attachment_is_covered(&second_attachment));
    assert_eq!(prepared_read(&client, &query).len(), 2);
    client.detach_query(first_attachment);
    client.detach_query(second_attachment);
}

#[test]
fn subscriber_connection_groups_duplicate_usage_subscriptions_by_coverage_key() {
    let schema = schema();
    let owner = AuthorId::from_bytes([0xa1; 16]);
    let client_author = AuthorId::from_bytes([0xc1; 16]);

    let server = open_core(0x5e, AuthorId::SYSTEM, &schema);
    let client = open_db(0xc1, client_author, &schema);

    seed(&server, "todos", cells("first", false, owner));

    let (client_transport, server_transport) = duplex();
    let _upstream = client.connect_upstream(client_transport);
    let subscriber = server.accept_subscriber(server_transport, client_author);

    let query = Query::from("todos");
    let prepared = prepared(&client, &query);
    let first_attachment = client
        .attach_query_with_opts(&prepared, global_subscribe_opts())
        .unwrap();
    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();

    let second_attachment = client
        .attach_query_with_opts(&prepared, global_subscribe_opts())
        .unwrap();
    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();

    let subscriber_ref = subscriber.borrow();
    let ConnectionLink::Subscriber {
        peer,
        served,
        coverage_groups,
        ..
    } = &subscriber_ref.link
    else {
        panic!("expected subscriber connection");
    };
    assert_eq!(served.len(), 2);
    assert_eq!(coverage_groups.len(), 1);
    let group = coverage_groups
        .values()
        .next()
        .expect("duplicate usage subscriptions should share one coverage group");
    assert_eq!(group.subscribers.len(), 2);
    let maintained_metrics = peer.maintained_subscription_view_metrics();
    assert_eq!(maintained_metrics.hits_out, 2);
    assert_eq!(maintained_metrics.footprint.result_rows, 1);
    assert_eq!(prepared_read(&client, &query).len(), 1);
    drop(subscriber_ref);
    client.detach_query(first_attachment);
    client.detach_query(second_attachment);
    client.tick().unwrap();
    server.tick().unwrap();
    let subscriber_ref = subscriber.borrow();
    let ConnectionLink::Subscriber {
        served,
        coverage_groups,
        ..
    } = &subscriber_ref.link
    else {
        panic!("expected subscriber connection");
    };
    assert!(served.is_empty());
    assert!(coverage_groups.is_empty());
}

#[test]
fn dropping_live_subscriptions_detaches_usage_subscriptions() {
    let schema = schema();
    let owner = AuthorId::from_bytes([0xa1; 16]);
    let client_author = AuthorId::from_bytes([0xc1; 16]);

    let server = open_core(0x5e, AuthorId::SYSTEM, &schema);
    let client = open_db(0xc1, client_author, &schema);

    seed(&server, "todos", cells("first", false, owner));

    let (client_transport, server_transport) = duplex();
    let _upstream = client.connect_upstream(client_transport);
    let subscriber = server.accept_subscriber(server_transport, client_author);

    let query = Query::from("todos");
    let mut first_subscription =
        prepared_subscribe(&client, &query, global_subscribe_opts()).unwrap();
    let mut second_subscription =
        prepared_subscribe(&client, &query, global_subscribe_opts()).unwrap();
    assert!(opened_rows(block_on(first_subscription.next_event()).unwrap()).is_empty());
    assert!(opened_rows(block_on(second_subscription.next_event()).unwrap()).is_empty());

    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();

    let subscriber_ref = subscriber.borrow();
    let ConnectionLink::Subscriber {
        served,
        coverage_groups,
        ..
    } = &subscriber_ref.link
    else {
        panic!("expected subscriber connection");
    };
    assert_eq!(served.len(), 1);
    assert_eq!(coverage_groups.len(), 1);
    let group = coverage_groups
        .values()
        .next()
        .expect("propagating subscriptions should share one forwarded coverage group");
    assert_eq!(group.subscribers.len(), 1);
    drop(subscriber_ref);

    drop(first_subscription);
    client.tick().unwrap();
    server.tick().unwrap();
    let subscriber_ref = subscriber.borrow();
    let ConnectionLink::Subscriber {
        served,
        coverage_groups,
        ..
    } = &subscriber_ref.link
    else {
        panic!("expected subscriber connection");
    };
    assert_eq!(served.len(), 1);
    assert_eq!(coverage_groups.len(), 1);
    drop(subscriber_ref);

    drop(second_subscription);
    client.tick().unwrap();
    server.tick().unwrap();
    let subscriber_ref = subscriber.borrow();
    let ConnectionLink::Subscriber {
        served,
        coverage_groups,
        ..
    } = &subscriber_ref.link
    else {
        panic!("expected subscriber connection");
    };
    assert!(served.is_empty());
    assert!(coverage_groups.is_empty());
}

#[test]
fn one_shot_edge_query_attaches_fresh_usage_subscription_for_covered_binding() {
    let schema = schema();
    let owner = AuthorId::from_bytes([0xa1; 16]);
    let client_author = AuthorId::from_bytes([0xc1; 16]);

    let server = open_core(0x5e, AuthorId::SYSTEM, &schema);
    let client = open_db(0xc1, client_author, &schema);

    seed(&server, "todos", cells("first", false, owner));

    let (client_transport, server_transport) = duplex();
    let _upstream = client.connect_upstream(client_transport);
    let _subscriber = server.accept_subscriber(server_transport, client_author);

    let query = Query::from("todos");
    let prepared = prepared(&client, &query);
    let first_attachment = client
        .attach_query_with_opts(&prepared, edge_subscribe_opts())
        .unwrap();
    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();
    assert!(client.query_attachment_is_covered(&first_attachment));
    assert_eq!(prepared_read(&client, &query).len(), 1);

    seed(&server, "todos", cells("second", false, owner));
    let second_attachment = client
        .attach_query_with_opts(&prepared, edge_subscribe_opts())
        .unwrap();
    assert!(client.query_attachment_is_covered(&first_attachment));
    assert!(!client.query_attachment_is_covered(&second_attachment));
    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();

    assert!(client.query_attachment_is_covered(&second_attachment));
    assert_eq!(prepared_read(&client, &query).len(), 2);
    client.detach_query(first_attachment);
    client.detach_query(second_attachment);
}

#[test]
fn missing_permissions_head_gates_sessions_but_not_trusted_backend_query_coverage() {
    // This stays at the transport boundary because the behavior under test is
    // the authenticated link's trust discriminator, which the public query API
    // deliberately does not expose.
    let schema = schema();
    let server = open_core(0x5e, AuthorId::SYSTEM, &schema);
    server.server.set_permissions_ready(false).unwrap();

    let backend_author = AuthorId::from_bytes([0xb0; 16]);
    let backend = open_db(0xb0, backend_author, &schema);
    let (backend_transport, server_backend_transport) = duplex();
    let _backend_upstream = backend.connect_upstream(backend_transport);
    let _backend_subscriber = server.accept_subscriber_with_trust(
        server_backend_transport,
        backend_author,
        CommitUnitTrust::TrustedBackend,
    );

    let session_author = AuthorId::from_bytes([0xc1; 16]);
    let session = open_db(0xc1, session_author, &schema);
    let (session_transport, server_session_transport) = duplex();
    let _session_upstream = session.connect_upstream(session_transport);
    let _session_subscriber = server.accept_subscriber(server_session_transport, session_author);

    let backend_query = prepared(&backend, &Query::from("todos"));
    let backend_attachment = backend
        .attach_query_with_opts(&backend_query, edge_subscribe_opts())
        .unwrap();
    let session_query = prepared(&session, &Query::from("todos"));
    let session_attachment = session
        .attach_query_with_opts(&session_query, edge_subscribe_opts())
        .unwrap();

    backend.tick().unwrap();
    session.tick().unwrap();
    server.tick().unwrap();
    backend.tick().unwrap();
    session.tick().unwrap();

    assert!(backend.query_attachment_is_covered(&backend_attachment));
    assert!(!session.query_attachment_is_covered(&session_attachment));

    server.server.set_permissions_ready(true).unwrap();
    server.tick().unwrap();
    session.tick().unwrap();
    assert!(session.query_attachment_is_covered(&session_attachment));
}

#[test]
fn one_shot_edge_query_attaches_fresh_claim_bound_usage_subscription_for_covered_binding() {
    let schema = JazzSchema::new([TableSchema::new(
        "chats",
        [
            ColumnSchema::new("title", ColumnType::String),
            ColumnSchema::new("joinCode", ColumnType::String.nullable()),
        ],
    )
    .with_read_policy(Policy::shape(
        Query::from("chats").filter(any_of([])).policy_branch(
            crate::query::PolicyBranch::single_alternative_from_query(
                Query::from("chats").filter(eq(col("joinCode"), crate::query::claim("join_code"))),
            ),
        ),
    ))
    .with_write_policy(Policy::public())]);
    let server = open_core(0x5e, AuthorId::SYSTEM, &schema);
    let reader = AuthorId::from_bytes([0xc1; 16]);
    let client = open_db(0xc1, reader, &schema);
    let join_code = "invite-code-123";
    client.set_identity_claims(
        reader,
        BTreeMap::from([("join_code".to_owned(), Value::String(join_code.to_owned()))]),
    );

    let first = seed(
        &server,
        "chats",
        BTreeMap::from([
            ("title".to_owned(), Value::String("first".to_owned())),
            (
                "joinCode".to_owned(),
                Value::Nullable(Some(Box::new(Value::String(join_code.to_owned())))),
            ),
        ]),
    );

    let (client_transport, server_transport) = duplex();
    let _upstream = client.connect_upstream(client_transport);
    let _subscriber = server.accept_subscriber_with_claims(
        server_transport,
        reader,
        BTreeMap::from([("join_code".to_owned(), Value::String(join_code.to_owned()))]),
    );

    let query = Query::from("chats");
    let prepared = prepared(&client, &query);
    let first_attachment = client
        .attach_query_with_opts(&prepared, edge_subscribe_opts())
        .unwrap();
    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();
    assert!(client.query_attachment_is_covered(&first_attachment));
    assert_eq!(
        row_ids(&prepared_all(&client, &query, edge_subscribe_opts())),
        vec![first]
    );

    let second = seed(
        &server,
        "chats",
        BTreeMap::from([
            ("title".to_owned(), Value::String("second".to_owned())),
            (
                "joinCode".to_owned(),
                Value::Nullable(Some(Box::new(Value::String(join_code.to_owned())))),
            ),
        ]),
    );
    let second_attachment = client
        .attach_query_with_opts(&prepared, edge_subscribe_opts())
        .unwrap();
    assert!(client.query_attachment_is_covered(&first_attachment));
    assert!(!client.query_attachment_is_covered(&second_attachment));
    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();

    assert!(client.query_attachment_is_covered(&second_attachment));
    assert_eq!(
        row_ids(&prepared_all(&client, &query, edge_subscribe_opts())),
        vec![first, second]
    );
    client.detach_query(first_attachment);
    client.detach_query(second_attachment);
}

#[test]
fn edge_subscription_with_claim_bound_policy_emits_later_matching_server_write() {
    let schema = JazzSchema::new([TableSchema::new(
        "chats",
        [
            ColumnSchema::new("title", ColumnType::String),
            ColumnSchema::new("joinCode", ColumnType::String.nullable()),
        ],
    )
    .with_read_policy(Policy::shape(
        Query::from("chats").filter(any_of([])).policy_branch(
            crate::query::PolicyBranch::single_alternative_from_query(
                Query::from("chats").filter(eq(col("joinCode"), crate::query::claim("join_code"))),
            ),
        ),
    ))
    .with_write_policy(Policy::public())]);
    let server = open_core(0x5e, AuthorId::SYSTEM, &schema);
    let reader = AuthorId::from_bytes([0xc1; 16]);
    let client = open_db(0xc1, reader, &schema);
    let join_code = "invite-code-123";
    let claims = BTreeMap::from([("join_code".to_owned(), Value::String(join_code.to_owned()))]);
    client.set_identity_claims(reader, claims.clone());

    let _first = seed(
        &server,
        "chats",
        BTreeMap::from([
            ("title".to_owned(), Value::String("first".to_owned())),
            (
                "joinCode".to_owned(),
                Value::Nullable(Some(Box::new(Value::String(join_code.to_owned())))),
            ),
        ]),
    );

    let (client_transport, server_transport) = duplex();
    let _upstream = client.connect_upstream(client_transport);
    let _subscriber = server.accept_subscriber_with_claims(server_transport, reader, claims);

    let query = Query::from("chats");
    let mut subscription = prepared_subscribe(&client, &query, edge_subscribe_opts()).unwrap();
    assert!(opened_rows(block_on(subscription.next_event()).unwrap()).is_empty());
    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();
    let SubscriptionEvent::Delta { added, .. } = block_on(subscription.next_event()).unwrap()
    else {
        panic!("expected subscription delta after upstream coverage");
    };
    assert_eq!(added.len(), 1);
}

#[test]
fn server_reset_subscription_materializes_without_local_snapshot_eval() {
    let schema = schema();
    let owner = AuthorId::from_bytes([0xa1; 16]);
    let client_author = AuthorId::from_bytes([0xc1; 16]);

    let server = open_core(0x5e, AuthorId::SYSTEM, &schema);
    let client = open_db(0xc1, client_author, &schema);

    seed(&server, "todos", cells("first", false, owner));
    seed(&server, "todos", cells("second", true, owner));

    let (client_transport, server_transport) = duplex();
    let _upstream = client.connect_upstream(client_transport);
    let _subscriber = server.accept_subscriber(server_transport, client_author);

    let query = Query::from("todos");
    let mut subscription = prepared_subscribe(&client, &query, global_subscribe_opts()).unwrap();
    assert!(opened_rows(block_on(subscription.next_event()).unwrap()).is_empty());

    client.tick().unwrap();
    server.tick().unwrap();
    client
        .node
        .node
        .borrow_mut()
        .reset_subscription_snapshot_for_link_call_count();
    let stats = client.tick_stats().unwrap();
    assert_eq!(stats.subscription_events, 1);
    assert_eq!(
        client
            .node
            .node
            .borrow()
            .subscription_snapshot_for_link_call_count(),
        0,
        "authoritative server reset should not re-run the subscription query locally"
    );

    let event = block_on(subscription.next_event()).unwrap();
    let SubscriptionEvent::Delta {
        reset,
        added,
        updated,
        removed,
        settled,
        ..
    } = event
    else {
        panic!("expected subscription delta");
    };
    assert!(reset);
    assert!(settled);
    assert_eq!(added.len(), 2);
    assert!(updated.is_empty());
    assert!(removed.is_empty());
}

#[test]
fn authoritative_reset_rebuilds_occurrence_sidecar_after_order_and_count_change() {
    let schema = schema();
    let client_author = AuthorId::from_bytes([0xc1; 16]);
    let client = open_db(0xc1, client_author, &schema);

    let first = row(0x71);
    let middle = row(0x72);
    let last = row(0x73);
    let first_write = client
        .insert_with_id("todos", first, cells("alpha", false, client_author))
        .unwrap();
    let _middle_write = client
        .insert_with_id("todos", middle, cells("middle", false, client_author))
        .unwrap();
    let last_write = client
        .insert_with_id("todos", last, cells("omega", false, client_author))
        .unwrap();
    client.tick().unwrap();

    let query = Query::from("todos").order_by("title", OrderDirection::Asc);
    let prepared = prepared(&client, &query);
    let opts = ReadOpts::default();
    let mut subscription = block_on(client.subscribe(&prepared, opts.clone())).unwrap();
    let SubscriptionEvent::Delta { added, .. } = block_on(subscription.next_event()).unwrap()
    else {
        panic!("expected opening subscription delta");
    };
    assert_eq!(
        added.iter().map(|row| row.row_uuid()).collect::<Vec<_>>(),
        vec![first, middle, last]
    );

    let first_updated = client
        .update(
            "todos",
            first,
            BTreeMap::from([("title".to_owned(), Value::String("zulu".to_owned()))]),
        )
        .unwrap();
    let binding_view_key = BindingViewKey::new(
        prepared.shape().shape_id(),
        prepared.binding().binding_id(),
        RegisterShapeOptions {
            tier: opts.tier,
            read_view: opts.read_view,
        }
        .read_view_key(),
    );
    client
        .node
        .node
        .borrow_mut()
        .inject_pending_authoritative_reset_for_test(
            binding_view_key,
            [
                ResultMemberEntry::row((
                    "todos".to_owned().into(),
                    first,
                    first_updated.mergeable_tx_id(),
                )),
                ResultMemberEntry::row((
                    "todos".to_owned().into(),
                    last,
                    last_write.mergeable_tx_id(),
                )),
            ],
            GlobalSeq(42),
        );

    assert_eq!(client.refresh_subscriptions().unwrap(), 1);
    let event = block_on(subscription.next_event()).unwrap();
    let reset = if matches!(event, SubscriptionEvent::Delta { reset: true, .. }) {
        event
    } else {
        block_on(subscription.next_event()).unwrap()
    };
    assert!(matches!(
        reset,
        SubscriptionEvent::Delta { reset: true, .. }
    ));

    let state = subscription._state.borrow();
    let SubscriptionKind::Prepared {
        maintained_subscription: Some(maintained),
        ..
    } = &state.kind
    else {
        panic!("expected maintained subscription state");
    };
    let paired = subscription_outputs_with_occurrence_sidecar(
        &state.snapshot,
        maintained.root_occurrence_ids(),
    )
    .expect("authoritative reset must atomically replace the root occurrence sidecar");
    assert_eq!(
        paired
            .iter()
            .map(|output| output.row_uuid())
            .collect::<Vec<_>>(),
        vec![last, first],
        "reset rows reordered after the title update and removed the middle row"
    );
    assert_eq!(
        paired
            .iter()
            .map(|output| output.occurrence_id.clone())
            .collect::<Vec<_>>(),
        vec![
            OutputOccurrenceId::single_source(ObjectId::from_uuid(last.0)),
            OutputOccurrenceId::single_source(ObjectId::from_uuid(first.0)),
        ],
        "each reset row remains paired with its current occurrence root"
    );
    assert_ne!(
        first_write.mergeable_tx_id(),
        first_updated.mergeable_tx_id()
    );
}

#[test]
fn authoritative_reset_with_missing_payload_falls_back_to_refresh() {
    let schema = schema();
    let client_author = AuthorId::from_bytes([0xc1; 16]);
    let server = open_core(0x5e, AuthorId::SYSTEM, &schema);
    let client = open_db(0xc1, client_author, &schema);

    let (client_transport, server_transport) = duplex();
    let _upstream = client.connect_upstream(client_transport);
    let _subscriber = server.accept_subscriber(server_transport, client_author);

    let query = Query::from("todos");
    let prepared = prepared(&client, &query);
    let opts = global_subscribe_opts();
    let mut subscription = block_on(client.subscribe(&prepared, opts.clone())).unwrap();
    assert!(opened_rows(block_on(subscription.next_event()).unwrap()).is_empty());

    let missing_tx = TxId::new(
        TxTime(116_898_697_390_129_152),
        NodeUuid::from_bytes([0x77; 16]),
    );
    let binding_view_key = BindingViewKey::new(
        prepared.shape().shape_id(),
        prepared.binding().binding_id(),
        RegisterShapeOptions {
            tier: opts.tier,
            read_view: opts.read_view,
        }
        .read_view_key(),
    );
    client
        .node
        .node
        .borrow_mut()
        .inject_pending_authoritative_reset_for_test(
            binding_view_key,
            [ResultMemberEntry::row((
                "todos".to_owned().into(),
                row(0x7a),
                missing_tx,
            ))],
            GlobalSeq(42),
        );
    client
        .node
        .node
        .borrow_mut()
        .reset_subscription_snapshot_for_link_call_count();

    let changed = client.refresh_subscriptions().unwrap();
    assert_eq!(changed, 1);
    let node = client.node.node.borrow();
    assert_eq!(
        node.sync_metrics()
            .authoritative_reset_missing_payload_fallbacks,
        1
    );
    assert_eq!(node.subscription_snapshot_for_link_call_count(), 1);
    assert!(
        node.has_pending_authoritative_reset_for_test(binding_view_key),
        "missing payload fallback must keep the authoritative reset pending for a later retry"
    );
    drop(node);
    assert!(prepared_all(&client, &query, ReadOpts::default()).is_empty());
}

#[test]
fn authoritative_reset_skips_stale_member_without_falling_back() {
    let schema = schema();
    let client_author = AuthorId::from_bytes([0xc1; 16]);
    let client = open_db(0xc1, client_author, &schema);

    let query = Query::from("todos");
    let prepared = prepared(&client, &query);
    let opts = global_subscribe_opts();
    let mut subscription = block_on(client.subscribe(&prepared, opts.clone())).unwrap();
    assert!(opened_rows(block_on(subscription.next_event()).unwrap()).is_empty());

    let live_row = row(0x7a);
    let stale_row = row(0x7b);
    let tx_id = client
        .node
        .node
        .borrow_mut()
        .commit_mergeable(
            MergeableCommit::new("todos", live_row, client.next_now_ms())
                .made_by(client_author)
                .permission_subject(client_author)
                .cells(cells("live", false, client_author)),
        )
        .unwrap();

    let binding_view_key = BindingViewKey::new(
        prepared.shape().shape_id(),
        prepared.binding().binding_id(),
        RegisterShapeOptions {
            tier: opts.tier,
            read_view: opts.read_view,
        }
        .read_view_key(),
    );
    client
        .node
        .node
        .borrow_mut()
        .inject_pending_authoritative_reset_for_test(
            binding_view_key,
            [
                ResultMemberEntry::row(("todos".to_owned().into(), live_row, tx_id)),
                ResultMemberEntry::row(("todos".to_owned().into(), stale_row, tx_id)),
            ],
            GlobalSeq(42),
        );
    client
        .node
        .node
        .borrow_mut()
        .reset_subscription_snapshot_for_link_call_count();

    let changed = client.refresh_subscriptions().unwrap();
    assert_eq!(changed, 1);
    assert_eq!(
        client
            .node
            .node
            .borrow()
            .subscription_snapshot_for_link_call_count(),
        0,
        "stale members with present tx metadata must not force local query fallback"
    );
    let event = block_on(subscription.next_event()).unwrap();
    let SubscriptionEvent::Delta {
        reset,
        added,
        updated,
        removed,
        settled,
        ..
    } = event
    else {
        panic!("expected subscription delta");
    };
    assert!(reset);
    assert!(settled);
    assert!(updated.is_empty());
    assert!(removed.is_empty());
    assert_eq!(added.len(), 1);
    assert_eq!(added[0].row_uuid(), live_row);
}

#[test]
fn client_tier_routing_scans_local_overlay_but_uses_global_settled_members_at_edge() {
    // The client holds an extra raw row locally while the serving host has
    // only the published row. This guards against an Edge facade widening
    // server scope by re-scanning a broad local transport cache.
    let schema = schema();
    let client_author = AuthorId::from_bytes([0xc1; 16]);
    let server = open_core(0x5e, AuthorId::SYSTEM, &schema);
    let db = open_db(0xc1, client_author, &schema);
    let published = seed(&server, "todos", cells("published", false, client_author));
    let server_overemitted = row(0x72);
    let published_tx = db
        .node
        .node
        .borrow_mut()
        .commit_mergeable(
            MergeableCommit::new("todos", published, db.next_now_ms())
                .made_by(client_author)
                .permission_subject(client_author)
                .cells(cells("not published", false, client_author)),
        )
        .unwrap();
    let overemitted_tx = db
        .node
        .node
        .borrow_mut()
        .commit_mergeable(
            MergeableCommit::new("todos", server_overemitted, db.next_now_ms())
                .made_by(client_author)
                .permission_subject(client_author)
                .cells(cells("published", false, client_author)),
        )
        .unwrap();
    {
        let mut node = db.node.node.borrow_mut();
        node.apply_fate_update(
            published_tx,
            Fate::Accepted,
            None,
            Some(DurabilityTier::Edge),
        )
        .unwrap();
        node.apply_fate_update(
            overemitted_tx,
            Fate::Accepted,
            None,
            Some(DurabilityTier::Edge),
        )
        .unwrap();
    }

    let query = Query::from("todos").filter(in_list(
        col("id"),
        [lit(published.0), lit(server_overemitted.0)],
    ));
    let prepared = prepared(&db, &query);
    let ids = |rows: Vec<CurrentRow>| {
        rows.into_iter()
            .map(|row| row.row_uuid())
            .collect::<BTreeSet<_>>()
    };
    let none_opts = ReadOpts {
        tier: DurabilityTier::None,
        local_updates: LocalUpdates::Deferred,
        propagation: Propagation::LocalOnly,
        include_deleted: false,
        ..ReadOpts::default()
    };
    let local_opts = ReadOpts {
        tier: DurabilityTier::Local,
        local_updates: LocalUpdates::Deferred,
        propagation: Propagation::LocalOnly,
        include_deleted: false,
        ..ReadOpts::default()
    };
    assert_eq!(
        ids(block_on(db.all(&prepared, none_opts)).unwrap()),
        BTreeSet::from([published, server_overemitted]),
        "None reads scan the complete process-local overlay"
    );
    assert_eq!(
        ids(block_on(db.all(&prepared, local_opts)).unwrap()),
        BTreeSet::from([published, server_overemitted]),
        "Local reads scan the complete durable local overlay"
    );

    let (client_transport, server_transport) = duplex();
    let _upstream = db.connect_upstream(client_transport);
    let _subscriber = server.accept_subscriber(server_transport, client_author);
    let attachment = db
        .attach_query_with_opts(&prepared, edge_subscribe_opts())
        .expect("attach Edge coverage");
    db.tick().unwrap();
    server.tick().unwrap();
    db.tick().unwrap();
    assert!(db.query_attachment_is_covered(&attachment));

    // Coverage acknowledgements are usage-site scoped. A second attachment
    // shares the canonical Global result set, but must wait for its own server
    // response rather than treating the older attachment's empty/non-empty
    // state as fresh coverage.
    let fresh_attachment = db
        .attach_query_with_opts(&prepared, edge_subscribe_opts())
        .expect("attach a second Edge coverage request");
    db.tick().unwrap();
    let concurrent_attachment = db
        .attach_query_with_opts(&prepared, edge_subscribe_opts())
        .expect("attach concurrent Edge coverage");
    db.tick().unwrap();
    assert!(
        !db.query_attachment_is_covered(&fresh_attachment),
        "a prior canonical result set must not acknowledge a new attachment"
    );
    assert!(
        !db.query_attachment_is_covered(&concurrent_attachment),
        "concurrent attachments require a later shared receipt"
    );
    db.tick().unwrap();
    server.tick().unwrap();
    db.tick().unwrap();
    assert!(db.query_attachment_is_covered(&fresh_attachment));
    assert!(db.query_attachment_is_covered(&concurrent_attachment));
    db.detach_query(fresh_attachment);
    db.detach_query(concurrent_attachment);

    assert_eq!(
        ids(block_on(db.all(&prepared, edge_subscribe_opts())).unwrap()),
        BTreeSet::from([published]),
        "Edge reads consume the canonical Global settled member set"
    );
    assert_eq!(
        ids(block_on(db.all(&prepared, global_subscribe_opts())).unwrap()),
        BTreeSet::from([published]),
        "Global reads consume the canonical Global settled member set"
    );
    db.detach_query(attachment);
    let reattached = db
        .attach_query_with_opts(&prepared, edge_subscribe_opts())
        .expect("re-attach Edge coverage after unsubscribe");
    db.tick().unwrap();
    assert!(
        !db.query_attachment_is_covered(&reattached),
        "unsubscribe then re-attach requires a newer receipt"
    );
    server.tick().unwrap();
    db.tick().unwrap();
    assert!(db.query_attachment_is_covered(&reattached));
    db.detach_query(reattached);
    let mut edge_subscription =
        block_on(db.subscribe(&prepared, edge_subscribe_opts())).expect("open edge subscription");
    assert!(opened_rows(block_on(edge_subscription.next_event()).unwrap()).is_empty());
    db.tick().unwrap();
    server.tick().unwrap();
    db.tick().unwrap();
    assert_eq!(
        ids(opened_rows(
            block_on(edge_subscription.next_event()).unwrap()
        )),
        BTreeSet::from([published]),
        "Edge maintained facades consume Global result members instead of raw local rows"
    );
    let refresh_attachment = db
        .attach_query_with_opts(&prepared, edge_subscribe_opts())
        .expect("refresh a deduplicated Edge attachment");
    db.tick().unwrap();
    assert!(
        !db.query_attachment_is_covered(&refresh_attachment),
        "a deduplicated attachment must request a later logical receipt"
    );
    server.tick().unwrap();
    db.tick().unwrap();
    assert!(db.query_attachment_is_covered(&refresh_attachment));
    db.detach_query(refresh_attachment);
    assert_eq!(
        ids(
            block_on(db.all_for_identity(&prepared, edge_subscribe_opts(), AuthorId::SYSTEM,))
                .unwrap()
        ),
        BTreeSet::from([published, server_overemitted]),
        "serving hosts remain TrustedServing and do not consume a client result cache"
    );
}

#[test]
fn client_settled_file_member_materializes_bundle_for_bound_id_read() {
    let schema = JazzSchema::new([
        TableSchema::new(
            "files",
            [
                crate::schema::ColumnSchema::new("mime_type", ColumnType::String),
                crate::schema::ColumnSchema::blob("data"),
            ],
        )
        .with_read_policy(Policy::public())
        .with_write_policy(Policy::public()),
        TableSchema::new(
            "attachments",
            [crate::schema::ColumnSchema::new(
                "file_id",
                ColumnType::Uuid,
            )],
        )
        .with_reference("file_id", "files")
        .with_read_policy(Policy::public())
        .with_write_policy(Policy::public()),
    ]);
    let client_author = AuthorId::from_bytes([0xc2; 16]);
    let server = open_core(0x5f, AuthorId::SYSTEM, &schema);
    let db = open_db(0xc2, client_author, &schema);
    let bytes = vec![0, 1, 9, 3, 255, 64, 128, 200];
    let file = seed(
        &server,
        "files",
        BTreeMap::from([
            (
                "mime_type".to_owned(),
                Value::String("application/x-proof".to_owned()),
            ),
            ("data".to_owned(), Value::Bytes(bytes.clone())),
        ]),
    );
    // Keep an attachment-shaped policy-evidence row in the serving snapshot:
    // the file payload must still be materialized from the file member itself.
    seed(
        &server,
        "attachments",
        BTreeMap::from([("file_id".to_owned(), Value::Uuid(file.0))]),
    );
    let query = Query::from("files").filter(eq(col("id"), lit(file.0)));
    let prepared = prepared(&db, &query);
    let (client_transport, server_transport) = duplex();
    let _upstream = db.connect_upstream(client_transport);
    let _subscriber = server.accept_subscriber(server_transport, client_author);
    let attachment = db
        .attach_query_with_opts(&prepared, edge_subscribe_opts())
        .expect("attach file coverage");
    db.tick().unwrap();
    server.tick().unwrap();
    db.tick().unwrap();
    assert!(db.query_attachment_is_covered(&attachment));
    let rows = block_on(db.all(&prepared, edge_subscribe_opts())).unwrap();
    assert_eq!(
        rows.len(),
        1,
        "settled file member must materialize as an Edge row"
    );
    assert_eq!(rows[0].row_uuid(), file);
    let table = schema
        .tables
        .iter()
        .find(|table| table.name == "files")
        .unwrap();
    let Value::Bytes(handle) = rows[0].cell(table, "data").unwrap() else {
        panic!("file data must be a large-value handle");
    };
    assert!(
        !handle.is_empty(),
        "the received file row retains its content handle"
    );
}

#[test]
fn propagated_authoritative_reset_uses_delivered_binding_view() {
    let schema = schema();
    let client_author = AuthorId::from_bytes([0xc1; 16]);
    let client = open_db(0xc1, client_author, &schema);

    let query = Query::from("todos");
    let prepared = prepared(&client, &query);
    let opts = ReadOpts {
        tier: DurabilityTier::Local,
        local_updates: LocalUpdates::Deferred,
        propagation: Propagation::Full,
        include_deleted: false,
        ..ReadOpts::default()
    };
    let mut subscription = block_on(client.subscribe(&prepared, opts.clone())).unwrap();
    assert!(opened_rows(block_on(subscription.next_event()).unwrap()).is_empty());

    let live_row = row(0x7c);
    let tx_id = client
        .node
        .node
        .borrow_mut()
        .commit_mergeable(
            MergeableCommit::new("todos", live_row, client.next_now_ms())
                .made_by(client_author)
                .permission_subject(client_author)
                .cells(cells("delivered reset", false, client_author)),
        )
        .unwrap();
    let delivered_binding_view_key = BindingViewKey::new(
        prepared.shape().shape_id(),
        prepared.binding().binding_id(),
        RegisterShapeOptions {
            tier: opts.tier,
            read_view: opts.read_view,
        }
        .read_view_key(),
    );
    client
        .node
        .node
        .borrow_mut()
        .inject_pending_authoritative_reset_for_test(
            delivered_binding_view_key,
            [ResultMemberEntry::row((
                "todos".to_owned().into(),
                live_row,
                tx_id,
            ))],
            GlobalSeq(42),
        );
    client
        .node
        .node
        .borrow_mut()
        .reset_subscription_snapshot_for_link_call_count();

    let changed = client.refresh_subscriptions().unwrap();
    assert_eq!(changed, 1);
    assert_eq!(
        client
            .node
            .node
            .borrow()
            .subscription_snapshot_for_link_call_count(),
        0,
        "propagated resets are delivered under the app subscription binding view, not the upstream global coverage key"
    );
    let event = block_on(subscription.next_event()).unwrap();
    let SubscriptionEvent::Delta {
        reset,
        added,
        updated,
        removed,
        settled,
        ..
    } = event
    else {
        panic!("expected subscription delta");
    };
    assert!(reset);
    assert!(
        !settled,
        "this synthetic unit injects only the delivered binding-view reset; real upstream traffic also advances the global coverage settle stamp"
    );
    assert!(updated.is_empty());
    assert!(removed.is_empty());
    assert_eq!(added.len(), 1);
    assert_eq!(added[0].row_uuid(), live_row);
}

#[test]
fn write_state_waiter_resolves_on_remote_fate_update() {
    let schema = schema();
    let owner = AuthorId::from_bytes([0xa1; 16]);
    let client_author = AuthorId::from_bytes([0xc1; 16]);

    let server = open_core(0x5e, AuthorId::SYSTEM, &schema);
    let client = open_db(0xc1, client_author, &schema);

    let (client_transport, server_transport) = duplex();
    let _upstream = client.connect_upstream(client_transport);
    let _subscriber = server.accept_subscriber(server_transport, client_author);

    let write = client
        .insert("todos", cells("wait for fate", false, owner))
        .unwrap();
    let tx_id = write.mergeable_tx_id();
    assert_eq!(
        client.write_state(tx_id).unwrap().durability,
        DurabilityTier::Local
    );

    let changed = client.next_write_state_change(tx_id);
    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();
    block_on(changed);

    let state = client.write_state(tx_id).unwrap();
    assert_eq!(state.fate, Fate::Accepted);
    assert_eq!(state.durability, DurabilityTier::Global);
}

#[test]
fn db_sync_surface_round_trips_blob_large_value_to_reader() {
    let schema =
        JazzSchema::new([
            TableSchema::new("files", [crate::schema::ColumnSchema::blob("data")])
                .with_read_policy(Policy::public())
                .with_write_policy(Policy::public()),
        ]);
    let writer_author = AuthorId::from_bytes([0xc1; 16]);
    let reader_author = AuthorId::from_bytes([0xc2; 16]);
    let server = open_core(0x5e, AuthorId::SYSTEM, &schema);
    let writer = open_db(0xc1, writer_author, &schema);
    let reader = open_db(0xc2, reader_author, &schema);

    let (writer_transport, server_writer_transport) = duplex();
    let _writer_upstream = writer.connect_upstream(writer_transport);
    let _writer_subscriber = server.accept_subscriber(server_writer_transport, writer_author);
    let payload = b"synced blob bytes".to_vec();
    writer
        .insert(
            "files",
            BTreeMap::from([("data".to_owned(), Value::Bytes(payload.clone()))]),
        )
        .unwrap();
    writer.tick().unwrap();
    server.tick().unwrap();

    let (reader_transport, server_reader_transport) = duplex();
    let _reader_upstream = reader.connect_upstream(reader_transport);
    let _reader_subscriber = server.accept_subscriber(server_reader_transport, reader_author);
    let query = Query::from("files");
    let mut subscription = prepared_subscribe(&reader, &query, global_subscribe_opts()).unwrap();
    assert!(opened_rows(block_on(subscription.next_event()).unwrap()).is_empty());
    reader.tick().unwrap();
    server.tick().unwrap();
    reader.tick().unwrap();

    let table = &schema.tables[0];
    let (added, updated, removed) = delta_rows(block_on(subscription.next_event()).unwrap());
    assert_eq!(added.len(), 1);
    assert!(updated.is_empty());
    assert!(removed.is_empty());
    let handle = prepared_read(&reader, &query)[0].cell(table, "data");
    let Some(Value::Bytes(handle)) = handle else {
        panic!("expected large-value handle");
    };
    reader
        .hydrate_large_value_handle(&handle)
        .expect_err("large-value handle should be unhydrated before explicit fetch response");
    server.tick().unwrap();
    reader.tick().unwrap();
    assert_eq!(reader.hydrate_large_value_handle(&handle).unwrap(), payload);
}

#[test]
fn db_sync_surface_preserves_creator_provenance_across_peer_update() {
    let schema = schema();
    let alice = AuthorId::from_bytes([0xa1; 16]);
    let bob = AuthorId::from_bytes([0xb2; 16]);
    let server = open_core(0x5e, AuthorId::SYSTEM, &schema);
    let receiver = open_db(0xc1, alice, &schema);

    let write = server
        .insert_attributed(alice, "todos", cells("created by alice", false, alice))
        .unwrap();
    let row = write.row_uuid();
    let query = Query::from("todos");
    let create_unit = server
        .node()
        .borrow_mut()
        .commit_unit_for(write.mergeable_tx_id())
        .unwrap();
    receiver
        .node
        .node
        .borrow_mut()
        .apply_sync_message(create_unit)
        .unwrap();

    server.next_now_ms.set(2);
    let bob_update = server
        .update_attributed(
            bob,
            "todos",
            row,
            BTreeMap::from([(
                "title".to_owned(),
                Value::String("updated by bob".to_owned()),
            )]),
        )
        .unwrap();
    block_on(bob_update.wait(DurabilityTier::Global)).unwrap();
    let server_rows = server.read(&query).unwrap();
    assert_eq!(server_rows.len(), 1);
    assert_eq!(
        server_rows[0].provenance().unwrap().unwrap().updated_by,
        bob
    );
    let update_unit = server
        .node()
        .borrow_mut()
        .commit_unit_for(bob_update.mergeable_tx_id())
        .unwrap();
    let SyncMessage::CommitUnit { tx, versions } = update_unit else {
        panic!("expected update commit unit");
    };
    assert_eq!(versions[0].created_by(), alice);
    assert_eq!(versions[0].updated_by(), bob);
    let receiver_updates = receiver
        .node
        .node
        .borrow_mut()
        .apply_sync_message(SyncMessage::CommitUnit { tx, versions })
        .unwrap();
    assert!(
        receiver_updates.iter().any(|message| {
            matches!(
                message,
                SyncMessage::FateUpdate {
                    fate: Fate::Accepted,
                    ..
                }
            )
        }),
        "receiver should accept the update, got {receiver_updates:?}"
    );
    let receiver_unit = receiver
        .node
        .node
        .borrow_mut()
        .commit_unit_for(bob_update.mergeable_tx_id())
        .unwrap();
    let SyncMessage::CommitUnit {
        versions: receiver_versions,
        ..
    } = receiver_unit
    else {
        panic!("expected receiver commit unit");
    };
    assert_eq!(receiver_versions[0].created_by(), alice);
    assert_eq!(receiver_versions[0].updated_by(), bob);

    let alice_rows = prepared_read(&receiver, &query);
    assert_eq!(alice_rows.len(), 1);
    assert_eq!(alice_rows[0].row_uuid(), row);
    let provenance = alice_rows[0]
        .provenance()
        .unwrap()
        .expect("current rows should carry provenance");
    assert_eq!(provenance.created_by, alice);
    assert_eq!(provenance.updated_by, bob);
    assert!(
        provenance.created_at < provenance.updated_at,
        "updating a row must preserve creator provenance while advancing updater provenance"
    );
}

#[test]
fn db_sync_surface_blob_values_follow_ordinary_row_permissions() {
    // This is intentionally a core sync-surface test: the public jazz-tools
    // query API does not yet expose blob values cleanly enough to assert the
    // bytes there, but the behavior is still user-visible once that API lands.
    let schema = owner_blob_schema();
    let alice = AuthorId::from_bytes([0xa1; 16]);
    let bob = AuthorId::from_bytes([0xb2; 16]);
    let mallory = AuthorId::from_bytes([0xc3; 16]);
    let server = open_core(0x5e, AuthorId::SYSTEM, &schema);
    let alice_db = open_db(0xa1, alice, &schema);
    let bob_db = open_db(0xb2, bob, &schema);
    let mallory_db = open_db(0xc3, mallory, &schema);

    let spoof = mallory_db.insert(
        "assets",
        BTreeMap::from([
            ("owner".to_owned(), Value::Uuid(alice.0)),
            (
                "mime_type".to_owned(),
                Value::String("application/octet-stream".to_owned()),
            ),
            ("data".to_owned(), Value::Bytes(b"spoofed".to_vec())),
        ]),
    );
    match spoof {
        Ok(_) => panic!("foreign owner blob insert should be rejected locally"),
        Err(error) => assert_eq!(error.code, ErrorCode::WriteRejected),
    }

    let (alice_transport, server_alice_transport) = duplex();
    let _alice_upstream = alice_db.connect_upstream(alice_transport);
    let _alice_subscriber = server.accept_subscriber(server_alice_transport, alice);

    let payload = b"file-like payload stored as an ordinary row value"
        .repeat(64)
        .to_vec();
    let write = alice_db
        .insert(
            "assets",
            BTreeMap::from([
                ("owner".to_owned(), Value::Uuid(alice.0)),
                (
                    "mime_type".to_owned(),
                    Value::String("application/octet-stream".to_owned()),
                ),
                ("data".to_owned(), Value::Bytes(payload.clone())),
            ]),
        )
        .unwrap();
    let asset = write.row_uuid();
    alice_db.tick().unwrap();
    server.tick().unwrap();
    alice_db.tick().unwrap();
    block_on(write.wait(DurabilityTier::Global)).unwrap();

    let query = Query::from("assets");
    let table = &schema.tables[0];
    let alice_rows = prepared_all(&alice_db, &query, global_subscribe_opts());
    assert_eq!(alice_rows.len(), 1);
    assert_eq!(alice_rows[0].row_uuid(), asset);
    let Some(Value::Bytes(handle)) = alice_rows[0].cell(table, "data") else {
        panic!("expected large-value handle");
    };
    assert_eq!(
        alice_db.hydrate_large_value_handle(&handle).unwrap(),
        payload
    );

    let (bob_transport, server_bob_transport) = duplex();
    let _bob_upstream = bob_db.connect_upstream(bob_transport);
    let _bob_subscriber = server.accept_subscriber(server_bob_transport, bob);
    let mut subscription = prepared_subscribe(&bob_db, &query, edge_subscribe_opts()).unwrap();
    assert!(opened_rows(block_on(subscription.next_event()).unwrap()).is_empty());
    assert!(prepared_all(&bob_db, &query, edge_subscribe_opts()).is_empty());
}

#[test]
fn db_sync_surface_edge_session_read_policy_filters_private_table_query() {
    let schema = owner_id_read_schema();
    let alice = AuthorId::from_bytes([0xa1; 16]);
    let bob = AuthorId::from_bytes([0xb2; 16]);
    let server = open_core(0x5e, AuthorId::SYSTEM, &schema);
    let writer = open_db(0xa1, alice, &schema);
    let reader = open_db(0xb2, bob, &schema);

    let (writer_transport, server_writer_transport) = duplex();
    let _writer_upstream = writer.connect_upstream(writer_transport);
    let _writer_subscriber = server.accept_subscriber_with_claims(
        server_writer_transport,
        alice,
        BTreeMap::from([("user_id".to_owned(), Value::String(alice.0.to_string()))]),
    );
    writer
        .insert(
            "messages",
            BTreeMap::from([
                ("body".to_owned(), Value::String("alice private".to_owned())),
                ("owner_id".to_owned(), Value::String(alice.0.to_string())),
            ]),
        )
        .unwrap();
    writer.tick().unwrap();
    server.tick().unwrap();

    let (reader_transport, server_reader_transport) = duplex();
    let _reader_upstream = reader.connect_upstream(reader_transport);
    let _reader_subscriber = server.accept_subscriber_with_claims(
        server_reader_transport,
        bob,
        BTreeMap::from([("user_id".to_owned(), Value::String(bob.0.to_string()))]),
    );
    let query = Query::from("messages");
    let mut subscription = prepared_subscribe(&reader, &query, edge_subscribe_opts()).unwrap();
    assert!(opened_rows(block_on(subscription.next_event()).unwrap()).is_empty());
    assert!(prepared_all(&reader, &query, edge_subscribe_opts()).is_empty());
}

#[test]
fn db_sync_surface_edge_session_read_policy_filters_after_runtime_schema_publish() {
    let public_schema = owner_id_public_schema();
    let permission_schema = owner_id_read_schema();
    let alice = AuthorId::from_bytes([0xa1; 16]);
    let bob = AuthorId::from_bytes([0xb2; 16]);
    let server = open_core(0x5e, AuthorId::SYSTEM, &public_schema);
    let writer = open_db(0xa1, alice, &permission_schema);
    let reader = open_db(0xb2, bob, &permission_schema);

    let schema_version = SchemaVersion::new(permission_schema.clone());
    let schema_id = schema_version.id;
    let acks = server.publish_schema(schema_version).unwrap();
    assert!(acks.into_iter().any(|message| matches!(
        message,
        SyncMessage::CatalogueAck(CatalogueAck {
            applied: true,
            schema: Some(applied_schema),
            ..
        }) if applied_schema == schema_id
    )));
    let current_acks = server
        .server
        .node()
        .borrow_mut()
        .apply_trusted_catalogue_message(SyncMessage::SetCurrentWriteSchema {
            author: AuthorId::SYSTEM,
            pointer: CurrentWriteSchema {
                revision: 1,
                schema: schema_id,
            },
        })
        .unwrap();
    assert!(current_acks.into_iter().any(|message| matches!(
        message,
        SyncMessage::CatalogueAck(CatalogueAck {
            applied: true,
            schema: Some(applied_schema),
            ..
        }) if applied_schema == schema_id
    )));

    let (writer_transport, server_writer_transport) = duplex();
    let _writer_upstream = writer.connect_upstream(writer_transport);
    let _writer_subscriber = server.accept_subscriber_with_claims(
        server_writer_transport,
        alice,
        BTreeMap::from([("user_id".to_owned(), Value::String(alice.0.to_string()))]),
    );
    writer
        .insert(
            "messages",
            BTreeMap::from([
                ("body".to_owned(), Value::String("alice private".to_owned())),
                ("owner_id".to_owned(), Value::String(alice.0.to_string())),
            ]),
        )
        .unwrap();
    writer.tick().unwrap();
    server.tick().unwrap();

    let (reader_transport, server_reader_transport) = duplex();
    let _reader_upstream = reader.connect_upstream(reader_transport);
    let _reader_subscriber = server.accept_subscriber_with_claims(
        server_reader_transport,
        bob,
        BTreeMap::from([("user_id".to_owned(), Value::String(bob.0.to_string()))]),
    );
    let query = Query::from("messages");
    let mut subscription = prepared_subscribe(&reader, &query, edge_subscribe_opts()).unwrap();
    assert!(opened_rows(block_on(subscription.next_event()).unwrap()).is_empty());

    assert!(prepared_all(&reader, &query, edge_subscribe_opts()).is_empty());
}

#[test]
fn detached_subscriber_is_not_served_on_server_tick() {
    let schema = schema();
    let owner = AuthorId::from_bytes([0xa1; 16]);
    let client_author = AuthorId::from_bytes([0xc1; 16]);

    let server = open_core(0x5e, AuthorId::SYSTEM, &schema);
    let client = open_db(0xc1, client_author, &schema);

    seed(&server, "todos", cells("from server", false, owner));

    let (client_transport, server_transport) = duplex();
    let _upstream = client.connect_upstream(client_transport);
    let subscriber = server.accept_subscriber(server_transport, client_author);

    let query = Query::from("todos");
    let mut subscription = prepared_subscribe(&client, &query, global_subscribe_opts()).unwrap();
    assert!(opened_rows(block_on(subscription.next_event()).unwrap()).is_empty());
    client.tick().unwrap();

    assert!(server.server.detach_connection(&subscriber));
    server.tick().unwrap();
    client.tick().unwrap();

    assert!(prepared_read(&client, &query).is_empty());
}

#[test]
fn byte_wire_round_trips_subscription_to_client() {
    let schema = schema();
    let owner = AuthorId::from_bytes([0xa1; 16]);
    let client_author = AuthorId::from_bytes([0xc1; 16]);

    let server = open_core(0x5e, AuthorId::SYSTEM, &schema);
    let client = open_db(0xc1, client_author, &schema);

    seed(&server, "todos", cells("from server", false, owner));

    let (client_bytes, server_bytes) = byte_duplex_raw();
    let server_inbound = Rc::clone(&server_bytes.inbound);
    let _upstream = client.connect_upstream(Box::new(WireTransportAdapter::current(client_bytes)));
    let _subscriber = server.accept_subscriber(
        Box::new(WireTransportAdapter::current(server_bytes)),
        client_author,
    );

    let query = Query::from("todos");
    let mut subscription = prepared_subscribe(&client, &query, global_subscribe_opts()).unwrap();
    assert!(opened_rows(block_on(subscription.next_event()).unwrap()).is_empty());

    client.tick().unwrap();
    {
        let queued = server_inbound.borrow();
        let first = queued.front().expect("register shape frame");
        let second = queued.get(1).expect("subscribe frame");
        let mut decoder = WireStreamDecoder::new(current_wire_features()).unwrap();
        let first = match decode_frame(first).unwrap() {
            WireFrame::Message(envelope) => decode_wire_message_payload(&mut decoder, &envelope),
            other => panic!("expected message frame, got {other:?}"),
        };
        let second = match decode_frame(second).unwrap() {
            WireFrame::Message(envelope) => decode_wire_message_payload(&mut decoder, &envelope),
            other => panic!("expected message frame, got {other:?}"),
        };
        let SyncMessage::RegisterShape { shape_id, .. } = first else {
            panic!("expected RegisterShape, got {first:?}");
        };
        let SyncMessage::Subscribe(subscribe) = second else {
            panic!("expected Subscribe, got {second:?}");
        };
        assert_eq!(subscribe.shape_id, shape_id);
        assert_eq!(subscribe.subscription.shape_id, shape_id);
    }
    server.tick().unwrap();
    client.tick().unwrap();

    let table = &schema.tables[0];
    let rows = prepared_read(&client, &query);
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].cell(table, "title"),
        Some(Value::String("from server".to_owned()))
    );
    let (added, updated, removed) = delta_rows(block_on(subscription.next_event()).unwrap());
    assert_eq!(added.len(), 1);
    assert!(updated.is_empty());
    assert!(removed.is_empty());

    seed(&server, "todos", cells("second", true, owner));
    server.tick().unwrap();
    client.tick().unwrap();
    assert_eq!(prepared_read(&client, &query).len(), 2);
}

#[test]
fn single_upstream_tick_applies_multiple_subscription_updates() {
    let schema = issue_schema();
    let owner = AuthorId::from_bytes([0xa1; 16]);
    let client_author = AuthorId::from_bytes([0xc1; 16]);

    let server = open_core(0x5e, AuthorId::SYSTEM, &schema);
    let client = open_db(0xc1, client_author, &schema);

    let project = row(1);
    server
        .insert_with_id(
            "projects",
            project,
            BTreeMap::from([("name".to_owned(), Value::String("Platform".to_owned()))]),
        )
        .unwrap();
    seed(
        &server,
        "issues",
        issue_cells("API", "open", owner, project, 5, &["api"], None),
    );

    let (client_transport, server_transport) = duplex();
    let _upstream = client.connect_upstream(client_transport);
    let _subscriber = server.accept_subscriber(server_transport, client_author);

    let projects = Query::from("projects");
    let issues = Query::from("issues");
    let mut project_subscription =
        prepared_subscribe(&client, &projects, global_subscribe_opts()).unwrap();
    let mut issue_subscription =
        prepared_subscribe(&client, &issues, global_subscribe_opts()).unwrap();
    assert!(opened_rows(block_on(project_subscription.next_event()).unwrap()).is_empty());
    assert!(opened_rows(block_on(issue_subscription.next_event()).unwrap()).is_empty());

    client.tick().unwrap();
    server.tick().unwrap();
    let stats = client.tick_stats().unwrap();

    assert_eq!(prepared_read(&client, &projects).len(), 1);
    assert_eq!(prepared_read(&client, &issues).len(), 1);
    assert_eq!(stats.subscription_events, 2);
    assert_eq!(
        delta_rows(block_on(project_subscription.next_event()).unwrap())
            .0
            .len(),
        1
    );
    assert_eq!(
        delta_rows(block_on(issue_subscription.next_event()).unwrap())
            .0
            .len(),
        1
    );
}

#[test]
fn subscriber_connection_serves_current_rows_and_resumes_from_cursor() {
    let schema = schema();
    let owner = AuthorId::from_bytes([0xa1; 16]);
    let client_author = AuthorId::from_bytes([0xc1; 16]);

    let server = open_core(0x5e, AuthorId::SYSTEM, &schema);
    let client = open_db(0xc1, client_author, &schema);

    seed(&server, "todos", cells("first", false, owner));
    seed(&server, "todos", cells("second", false, owner));

    let (client_transport, server_transport) = duplex();
    let _upstream = client.connect_upstream(client_transport);
    let subscriber = server.accept_subscriber(server_transport, client_author);
    let query = Query::from("todos");
    let mut subscription = prepared_subscribe(&client, &query, global_subscribe_opts()).unwrap();
    assert!(opened_rows(block_on(subscription.next_event()).unwrap()).is_empty());

    // The subscriber registers the whole-table query shape; explicit
    // current-row serving then sends the facade-level initial snapshot.
    client.tick().unwrap();
    subscriber.borrow_mut().serve_current_rows("todos").unwrap();
    client.tick().unwrap();

    let (added, updated, removed) = delta_rows(block_on(subscription.next_event()).unwrap());
    assert_eq!(added.len(), 2);
    assert!(updated.is_empty());
    assert!(removed.is_empty());
    let full_bytes = subscriber.borrow().last_resume_bytes().unwrap();
    assert!(full_bytes > 0);

    server.tick().unwrap();
    client.tick().unwrap();

    let third = seed(&server, "todos", cells("third", true, owner));
    server.tick().unwrap();
    client.tick().unwrap();
    assert_eq!(prepared_read(&client, &query).len(), 3);

    let cursor = subscriber.borrow_mut().take_resume_cursor().unwrap();
    let (client_transport, server_transport) = duplex();
    let _resumed_upstream = client.connect_upstream(client_transport);
    let resumed = server.accept_subscriber_with_resume(server_transport, client_author, cursor);

    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();

    let resume_bytes = resumed.borrow().last_resume_bytes().unwrap();
    assert!(
        resume_bytes > 0,
        "resume catch-up should send a bounded non-empty response after cursor resume"
    );
    assert!(
        resume_bytes <= full_bytes,
        "resume catch-up should stay bounded by the initial full response"
    );
    assert_eq!(prepared_read(&client, &query).len(), 3);
    assert!(
        prepared_read(&client, &query)
            .iter()
            .any(|row| row.row_uuid() == third)
    );
}

#[test]
fn byte_wire_subscriber_connection_serves_current_rows_and_resumes_from_cursor() {
    let schema = schema();
    let owner = AuthorId::from_bytes([0xa1; 16]);
    let client_author = AuthorId::from_bytes([0xc1; 16]);

    let server = open_core(0x5e, AuthorId::SYSTEM, &schema);
    let client = open_db(0xc1, client_author, &schema);

    seed(&server, "todos", cells("first", false, owner));
    seed(&server, "todos", cells("second", false, owner));

    let (client_transport, server_transport) = byte_duplex_with_session(client_author, 1);
    let _upstream = client.connect_upstream(client_transport);
    let subscriber = server.accept_subscriber(server_transport, client_author);
    let query = Query::from("todos");
    let mut subscription = prepared_subscribe(&client, &query, global_subscribe_opts()).unwrap();
    assert!(opened_rows(block_on(subscription.next_event()).unwrap()).is_empty());

    client.tick().unwrap();
    subscriber.borrow_mut().serve_current_rows("todos").unwrap();
    client.tick().unwrap();

    let (added, updated, removed) = delta_rows(block_on(subscription.next_event()).unwrap());
    assert_eq!(added.len(), 2);
    assert!(updated.is_empty());
    assert!(removed.is_empty());
    let full_bytes = subscriber.borrow().last_resume_bytes().unwrap();
    assert!(full_bytes > 0);

    server.tick().unwrap();
    client.tick().unwrap();

    let third = seed(&server, "todos", cells("third", true, owner));
    server.tick().unwrap();
    client.tick().unwrap();
    assert_eq!(prepared_read(&client, &query).len(), 3);

    let cursor = subscriber.borrow_mut().take_resume_cursor().unwrap();
    let (client_transport, server_transport) = byte_duplex_with_session(client_author, 2);
    let _resumed_upstream = client.connect_upstream(client_transport);
    let resumed = server.accept_subscriber_with_resume(server_transport, client_author, cursor);

    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();

    let resume_bytes = resumed.borrow().last_resume_bytes().unwrap();
    assert!(
        resume_bytes > 0,
        "byte-wire resume catch-up should send a bounded non-empty response after cursor resume"
    );
    assert!(
        resume_bytes <= full_bytes,
        "byte-wire resume catch-up should stay bounded by the initial full response"
    );
    assert_eq!(prepared_read(&client, &query).len(), 3);
    assert!(
        prepared_read(&client, &query)
            .iter()
            .any(|row| row.row_uuid() == third)
    );
}

#[test]
fn connect_upstream_announces_existing_subscriptions_on_first_tick() {
    let schema = schema();
    let client_author = AuthorId::from_bytes([0xc1; 16]);
    let client = open_db(0xc1, client_author, &schema);
    let (client_transport, mut upstream_transport) = duplex();

    let query = Query::from("todos").filter(eq(col("done"), lit(false)));
    let _subscription = prepared_subscribe(&client, &query, global_subscribe_opts()).unwrap();
    let _upstream = client.connect_upstream(client_transport);

    client.tick().unwrap();
    let first = upstream_transport.try_recv().unwrap();
    let second = upstream_transport.try_recv().unwrap();
    assert!(upstream_transport.try_recv().is_none());

    let SyncMessage::RegisterShape { shape_id, .. } = first else {
        panic!("expected existing subscription shape to be registered upstream first");
    };
    let SyncMessage::Subscribe(subscribe) = second else {
        panic!("expected existing subscription to be announced upstream second");
    };
    assert_eq!(subscribe.shape_id, shape_id);
    assert_eq!(subscribe.subscription.shape_id, shape_id);
}

// SessionClaims has no distinct public state once the receiving NodeState has
// ignored an identical map, so wire-count coverage must inspect the transport.
// The policy-visible integration coverage lives above this facade; this test
// protects the otherwise unobservable wire-chatter contract.
#[test]
fn repeated_identical_session_claims_emit_once_on_a_live_connection() {
    let schema = schema();
    let client_author = AuthorId::from_bytes([0xc1; 16]);
    let client = open_db(0xc1, client_author, &schema);
    let (client_transport, mut upstream_transport) = duplex();
    let _upstream = client.connect_upstream(client_transport);
    let claims = BTreeMap::from([("role".to_owned(), Value::String("reader".to_owned()))]);

    client.set_identity_claims(client_author, claims.clone());
    client.set_identity_claims(client_author, claims);
    client.tick().unwrap();

    assert!(matches!(
        upstream_transport.try_recv(),
        Some(SyncMessage::SessionClaims { .. })
    ));
    assert!(
        upstream_transport.try_recv().is_none(),
        "an unchanged claim map must not produce another wire message"
    );
}

// This is lower-level for the same reason as the wire-count test above. In
// particular, it is the regression that a global deduplication would miss:
// each newly attached transport must receive the current map independently.
#[test]
fn current_session_claims_reach_late_and_reconnected_upstreams() {
    let schema = schema();
    let client_author = AuthorId::from_bytes([0xc1; 16]);
    let client = open_db(0xc1, client_author, &schema);
    let claims = BTreeMap::from([("role".to_owned(), Value::String("reader".to_owned()))]);

    client.set_identity_claims(client_author, claims.clone());
    let (first_transport, mut first_upstream_transport) = duplex();
    let first_upstream = client.connect_upstream(first_transport);
    client.tick().unwrap();
    assert!(matches!(
        first_upstream_transport.try_recv(),
        Some(SyncMessage::SessionClaims { identity, claims: received })
            if identity == client_author && received == claims
    ));
    assert!(first_upstream_transport.try_recv().is_none());

    client.set_identity_claims(client_author, claims.clone());
    assert!(client.detach_connection(&first_upstream));

    let (reconnected_transport, mut reconnected_upstream_transport) = duplex();
    let _reconnected_upstream = client.connect_upstream(reconnected_transport);
    client.tick().unwrap();
    assert!(matches!(
        reconnected_upstream_transport.try_recv(),
        Some(SyncMessage::SessionClaims { identity, claims: received })
            if identity == client_author && received == claims
    ));
    assert!(reconnected_upstream_transport.try_recv().is_none());
}

#[test]
fn changed_session_claims_advance_delivery_after_an_identical_call() {
    let schema = schema();
    let client_author = AuthorId::from_bytes([0xc1; 16]);
    let client = open_db(0xc1, client_author, &schema);
    let (client_transport, mut upstream_transport) = duplex();
    let _upstream = client.connect_upstream(client_transport);
    let reader = BTreeMap::from([("role".to_owned(), Value::String("reader".to_owned()))]);
    let writer = BTreeMap::from([("role".to_owned(), Value::String("writer".to_owned()))]);

    client.set_identity_claims(client_author, reader.clone());
    client.tick().unwrap();
    assert!(matches!(
        upstream_transport.try_recv(),
        Some(SyncMessage::SessionClaims { claims, .. }) if claims == reader
    ));

    client.set_identity_claims(client_author, reader);
    client.tick().unwrap();
    assert!(upstream_transport.try_recv().is_none());

    client.set_identity_claims(client_author, writer.clone());
    client.tick().unwrap();
    assert!(matches!(
        upstream_transport.try_recv(),
        Some(SyncMessage::SessionClaims { identity, claims })
            if identity == client_author && claims == writer
    ));
    assert!(upstream_transport.try_recv().is_none());
}

#[test]
fn global_subscription_registers_array_subquery_upstream_coverage() {
    let schema = relation_schema();
    let client_author = AuthorId::from_bytes([0xc1; 16]);
    let client = open_db(0xc1, client_author, &schema);
    let (client_transport, mut upstream_transport) = duplex();
    let _upstream = client.connect_upstream(client_transport);

    let query = Query::from("users").array_subquery(
        ArraySubquery::new("todos", "todos", "owner_id", "id")
            .nested(ArraySubquery::new("comments", "comments", "todo_id", "id")),
    );
    let _subscription = prepared_subscribe(&client, &query, global_subscribe_opts()).unwrap();

    client.tick().unwrap();
    assert!(matches!(
        upstream_transport.try_recv(),
        Some(SyncMessage::RegisterShape { .. })
    ));
    assert!(matches!(
        upstream_transport.try_recv(),
        Some(SyncMessage::Subscribe(_))
    ));
}

#[test]
fn array_subquery_attachment_registers_upstream_coverage() {
    let schema = relation_schema();
    let client_author = AuthorId::from_bytes([0xc1; 16]);
    let client = open_db(0xc1, client_author, &schema);
    let (client_transport, mut upstream_transport) = duplex();
    let _upstream = client.connect_upstream(client_transport);

    let query = Query::from("users").array_subquery(
        ArraySubquery::new("todos", "todos", "owner_id", "id")
            .nested(ArraySubquery::new("comments", "comments", "todo_id", "id")),
    );
    let prepared = prepared(&client, &query);
    let attachment = client
        .attach_query_with_opts(&prepared, global_subscribe_opts())
        .unwrap();

    client.tick().unwrap();
    assert!(matches!(
        upstream_transport.try_recv(),
        Some(SyncMessage::RegisterShape { .. })
    ));
    assert!(matches!(
        upstream_transport.try_recv(),
        Some(SyncMessage::Subscribe(_))
    ));
    client.detach_query(attachment);
}

#[test]
fn upload_is_not_marked_sent_after_one_shot_backpressure_and_retries() {
    let schema = schema();
    let client_author = AuthorId::from_bytes([0xc1; 16]);
    let client = open_db(0xc1, client_author, &schema);
    let outbound = Rc::new(RefCell::new(std::collections::VecDeque::new()));
    let transport = BackpressureOnceTransport {
        outbound: Rc::clone(&outbound),
        failed: false,
    };
    let _upstream = client.connect_upstream(Box::new(transport));

    let tx_id = client
        .node
        .node
        .borrow_mut()
        .commit_mergeable(
            MergeableCommit::new("todos", row(0xf1), client.next_now_ms())
                .made_by(client_author)
                .permission_subject(client_author)
                .cells(cells("retry", false, client_author)),
        )
        .unwrap();
    client
        .node
        .outbox
        .borrow_mut()
        .push(PendingUpload { tx_id, unit: None });

    client.tick().unwrap();
    assert!(outbound.borrow().is_empty());
    assert_eq!(
        client
            .node
            .node
            .borrow()
            .sync_metrics()
            .transport_backpressure_retries,
        1
    );

    client.tick().unwrap();
    let sent = outbound.borrow_mut().pop_front().unwrap();
    let SyncMessage::CommitUnit { tx, .. } = sent else {
        panic!("expected retried commit upload");
    };
    assert_eq!(tx.tx_id, tx_id);
    assert!(outbound.borrow_mut().pop_front().is_none());
}

#[test]
fn local_missing_upload_body_still_kills_sync_driver() {
    let schema = schema();
    let client_author = AuthorId::from_bytes([0xc1; 16]);
    let client = open_db(0xc1, client_author, &schema);
    let (client_transport, _server_transport) = duplex();
    let _upstream = client.connect_upstream(client_transport);
    let missing_tx = TxId::new(
        crate::time::TxTime(client.next_now_ms()),
        NodeUuid::from_bytes([0xee; 16]),
    );
    client.node.outbox.borrow_mut().push(PendingUpload {
        tx_id: missing_tx,
        unit: None,
    });

    let error = client.tick().unwrap_err();
    assert_eq!(error.code, ErrorCode::Protocol);
    assert!(
        error.message.contains("missing transaction"),
        "unexpected local-fatal error: {}",
        error.message
    );
}

#[test]
fn blob_commit_upload_sends_content_extents_before_commit_unit() {
    let schema =
        JazzSchema::new([
            TableSchema::new("files", [crate::schema::ColumnSchema::blob("data")])
                .with_read_policy(Policy::public())
                .with_write_policy(Policy::public()),
        ]);
    let client_author = AuthorId::from_bytes([0xc1; 16]);
    let client = open_db(0xc1, client_author, &schema);
    let (client_transport, mut upstream_transport) = duplex();
    let _upstream = client.connect_upstream(client_transport);

    let write = client
        .insert(
            "files",
            BTreeMap::from([("data".to_owned(), Value::Bytes(b"blob bytes".to_vec()))]),
        )
        .unwrap();
    client.tick().unwrap();

    let first = upstream_transport.try_recv().unwrap();
    let second = upstream_transport.try_recv().unwrap();
    assert!(matches!(first, SyncMessage::ContentExtents { .. }));
    let SyncMessage::CommitUnit { tx, .. } = second else {
        panic!("expected commit unit after content extents");
    };
    assert_eq!(tx.tx_id, write.mergeable_tx_id());
}

#[test]
fn detach_connection_removes_connection_from_db_ticks() {
    let schema = schema();
    let client_author = AuthorId::from_bytes([0xc1; 16]);
    let client = open_db(0xc1, client_author, &schema);
    let (client_transport, mut upstream_transport) = duplex();

    let query = Query::from("todos").filter(eq(col("done"), lit(false)));
    let _subscription = prepared_subscribe(&client, &query, global_subscribe_opts()).unwrap();
    let upstream = client.connect_upstream(client_transport);

    assert!(client.detach_connection(&upstream));
    assert!(!client.detach_connection(&upstream));

    client.tick().unwrap();
    assert!(upstream_transport.try_recv().is_none());
}

#[test]
fn accepted_subscriber_is_served_under_subscriber_author_identity() {
    let schema = owner_read_schema();
    let subscriber_author = AuthorId::from_bytes([0xc1; 16]);
    let server_author = AuthorId::from_bytes([0x5e; 16]);
    let other_author = AuthorId::from_bytes([0xd1; 16]);
    let server = open_core(0x5e, server_author, &schema);
    let client = open_db(0xc1, subscriber_author, &schema);

    let visible = seed(
        &server,
        "todos",
        cells("for subscriber", false, subscriber_author),
    );
    seed(&server, "todos", cells("for server", false, server_author));
    seed(
        &server,
        "todos",
        cells("for someone else", false, other_author),
    );

    let (client_transport, server_transport) = duplex();
    let _upstream = client.connect_upstream(client_transport);
    let _subscriber = server.accept_subscriber(server_transport, subscriber_author);
    let query = Query::from("todos");
    let mut subscription = prepared_subscribe(&client, &query, global_subscribe_opts()).unwrap();
    assert!(opened_rows(block_on(subscription.next_event()).unwrap()).is_empty());

    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();

    let (rows, updated, removed) = delta_rows(block_on(subscription.next_event()).unwrap());
    assert!(updated.is_empty());
    assert!(removed.is_empty());
    assert_eq!(row_ids(&rows), vec![visible]);
    assert_eq!(
        rows[0].cell(&schema.tables[0], "title"),
        Some(Value::String("for subscriber".to_owned()))
    );
}

#[test]
fn maintained_subscription_emits_created_by_scoped_insert_after_empty_seed() {
    let schema = created_by_read_schema();
    let alice = AuthorId::from_bytes([0xa1; 16]);
    let db = open_db(0xa1, alice, &schema);
    let query = Query::from("todos");
    let prepared = prepared(&db, &query);
    let mut subscription = block_on(db.subscribe(&prepared, ReadOpts::default())).unwrap();

    assert!(opened_rows(block_on(subscription.next_event()).unwrap()).is_empty());

    let write = db
        .insert(
            "todos",
            BTreeMap::from([
                (
                    "title".to_owned(),
                    Value::String("created by alice".to_owned()),
                ),
                ("done".to_owned(), Value::Bool(false)),
            ]),
        )
        .unwrap();
    block_on(write.wait(DurabilityTier::Local)).unwrap();

    let one_shot = prepared_all(&db, &query, ReadOpts::default());
    assert_eq!(row_ids(&one_shot), vec![write.row_uuid()]);

    let (added, updated, removed) = delta_rows(block_on(subscription.next_event()).unwrap());
    assert_eq!(row_ids(&added), vec![write.row_uuid()]);
    assert!(updated.is_empty());
    assert!(removed.is_empty());
}

#[test]
fn prepared_one_shot_releases_local_groove_subscription_immediately() {
    let db = doctest_support::block_on(doctest_support::open_todos_db()).unwrap();
    let query = Query::from("todos").filter(eq(col("title"), param("title")));
    let prepared = db
        .prepare_query_bound(
            &query,
            BTreeMap::from([("title".to_owned(), Value::String("missing".to_owned()))]),
        )
        .unwrap();
    let baseline = db.runtime_stats_for_test().active_subscriptions;

    for _ in 0..4 {
        assert!(
            block_on(db.all(&prepared, ReadOpts::default()))
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            db.runtime_stats_for_test().active_subscriptions,
            baseline,
            "completed one-shot reads must not retain Groove outputs"
        );
    }
}

#[test]
fn dropping_local_stream_releases_groove_subscription_without_a_write() {
    let db = doctest_support::block_on(doctest_support::open_todos_db()).unwrap();
    let query = Query::from("todos").filter(eq(col("title"), param("title")));
    let prepared = db
        .prepare_query_bound(
            &query,
            BTreeMap::from([("title".to_owned(), Value::String("missing".to_owned()))]),
        )
        .unwrap();
    let baseline = db.runtime_stats_for_test().active_subscriptions;
    let opts = ReadOpts {
        propagation: Propagation::LocalOnly,
        ..ReadOpts::default()
    };

    let mut subscription = block_on(db.subscribe(&prepared, opts)).unwrap();
    assert!(opened_rows(block_on(subscription.next_event()).unwrap()).is_empty());
    assert_eq!(
        db.runtime_stats_for_test().active_subscriptions,
        baseline + 1
    );

    drop(subscription);
    assert_eq!(
        db.runtime_stats_for_test().active_subscriptions,
        baseline,
        "dropping a local stream must not wait for a later Groove notification"
    );
}

#[test]
fn dropping_one_local_stream_preserves_a_sibling_on_the_same_binding() {
    let db = doctest_support::block_on(doctest_support::open_todos_db()).unwrap();
    let query = Query::from("todos").filter(eq(col("title"), param("title")));
    let prepared = db
        .prepare_query_bound(
            &query,
            BTreeMap::from([("title".to_owned(), Value::String("match".to_owned()))]),
        )
        .unwrap();
    let baseline = db.runtime_stats_for_test().active_subscriptions;
    let opts = ReadOpts {
        propagation: Propagation::LocalOnly,
        ..ReadOpts::default()
    };
    let mut first = block_on(db.subscribe(&prepared, opts.clone())).unwrap();
    let mut survivor = block_on(db.subscribe(&prepared, opts)).unwrap();
    assert!(opened_rows(block_on(first.next_event()).unwrap()).is_empty());
    assert!(opened_rows(block_on(survivor.next_event()).unwrap()).is_empty());
    assert_eq!(
        db.runtime_stats_for_test().active_subscriptions,
        baseline + 2
    );

    drop(first);
    assert_eq!(
        db.runtime_stats_for_test().active_subscriptions,
        baseline + 1
    );

    let write = db
        .insert("todos", doctest_support::todo_cells("match", false))
        .unwrap();
    let (added, updated, removed) = delta_rows(block_on(survivor.next_event()).unwrap());
    assert_eq!(row_ids(&added), vec![write.row_uuid()]);
    assert!(updated.is_empty());
    assert!(removed.is_empty());
}

#[test]
fn maintained_subscription_emits_created_by_scoped_insert_for_explicit_identity() {
    let schema = created_by_read_schema();
    let alice = AuthorId::from_bytes([0xa1; 16]);
    let db = open_db(0xa1, alice, &schema);
    let query = Query::from("todos");
    let prepared = prepared(&db, &query);
    let mut subscription =
        block_on(db.subscribe_for_identity(&prepared, ReadOpts::default(), alice)).unwrap();

    assert!(opened_rows(block_on(subscription.next_event()).unwrap()).is_empty());

    let write = db
        .insert(
            "todos",
            BTreeMap::from([
                (
                    "title".to_owned(),
                    Value::String("created by alice".to_owned()),
                ),
                ("done".to_owned(), Value::Bool(false)),
            ]),
        )
        .unwrap();
    block_on(write.wait(DurabilityTier::Local)).unwrap();

    let one_shot = block_on(db.all_for_identity(&prepared, ReadOpts::default(), alice)).unwrap();
    assert_eq!(row_ids(&one_shot), vec![write.row_uuid()]);

    let (added, updated, removed) = delta_rows(block_on(subscription.next_event()).unwrap());
    assert_eq!(row_ids(&added), vec![write.row_uuid()]);
    assert!(updated.is_empty());
    assert!(removed.is_empty());
}

#[test]
fn local_propagating_subscription_emits_created_by_scoped_insert_after_empty_seed() {
    let schema = created_by_read_schema();
    let alice = AuthorId::from_bytes([0xa1; 16]);
    let server = open_core(0x5e, AuthorId::SYSTEM, &schema);
    let client = open_db(0xa1, alice, &schema);
    let (client_transport, server_transport) = duplex();
    let _upstream = client.connect_upstream(client_transport);
    let _subscriber = server.accept_subscriber(server_transport, alice);
    let query = Query::from("todos");
    let mut subscription = prepared_subscribe(&client, &query, ReadOpts::default()).unwrap();

    assert!(opened_rows(block_on(subscription.next_event()).unwrap()).is_empty());
    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();
    let mut snapshot = RelationSnapshot::default();
    while let Some(event) = subscription.try_next_event() {
        apply_subscription_event(&mut snapshot, event);
        assert!(
            snapshot.rows.is_empty(),
            "pre-insert coverage events must stay empty"
        );
    }

    let write = client
        .insert(
            "todos",
            BTreeMap::from([
                (
                    "title".to_owned(),
                    Value::String("created by alice".to_owned()),
                ),
                ("done".to_owned(), Value::Bool(false)),
            ]),
        )
        .unwrap();
    block_on(write.wait(DurabilityTier::Local)).unwrap();

    let one_shot = prepared_all(&client, &query, ReadOpts::default());
    assert_eq!(row_ids(&one_shot), vec![write.row_uuid()]);

    let (added, updated, removed) = delta_rows(block_on(subscription.next_event()).unwrap());
    assert_eq!(row_ids(&added), vec![write.row_uuid()]);
    assert!(updated.is_empty());
    assert!(removed.is_empty());
}

#[test]
fn local_propagating_subscription_coerces_user_id_claim_for_created_by() {
    let schema = created_by_read_schema_for_claim("user_id");
    let alice = AuthorId::from_bytes([0xa1; 16]);
    let server = open_core(0x5e, AuthorId::SYSTEM, &schema);
    let client = open_db(0xa1, alice, &schema);
    let claims = BTreeMap::from([("user_id".to_owned(), Value::String(alice.0.to_string()))]);
    client.set_identity_claims(alice, claims.clone());
    let (client_transport, server_transport) = duplex();
    let _upstream = client.connect_upstream(client_transport);
    let _subscriber = server.accept_subscriber_with_claims(server_transport, alice, claims);
    let query = Query::from("todos");
    let mut subscription = prepared_subscribe(&client, &query, ReadOpts::default()).unwrap();

    assert!(opened_rows(block_on(subscription.next_event()).unwrap()).is_empty());
    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();
    while let Some(event) = subscription.try_next_event() {
        assert!(opened_rows(event).is_empty());
    }

    let write = client
        .insert(
            "todos",
            doctest_support::todo_cells("created by alice", false),
        )
        .unwrap();
    block_on(write.wait(DurabilityTier::Local)).unwrap();

    let one_shot = prepared_all(&client, &query, ReadOpts::default());
    assert_eq!(row_ids(&one_shot), vec![write.row_uuid()]);
    let (added, updated, removed) = delta_rows(block_on(subscription.next_event()).unwrap());
    assert_eq!(row_ids(&added), vec![write.row_uuid()]);
    assert!(updated.is_empty());
    assert!(removed.is_empty());
}

fn resource_test_cells(title: &str) -> RowCells {
    resource_test_cells_with_group(title, row(0x11))
}

fn resource_test_cells_with_group(title: &str, group: RowUuid) -> RowCells {
    BTreeMap::from([
        ("org_id".to_owned(), Value::Uuid(row(0x01).0)),
        ("created_by".to_owned(), Value::Uuid(group.0)),
        ("updated_by".to_owned(), Value::Uuid(group.0)),
        ("archived".to_owned(), Value::Bool(false)),
        ("label".to_owned(), Value::String(title.to_owned())),
        ("date_created".to_owned(), Value::U64(1)),
        ("date_updated".to_owned(), Value::U64(2)),
        ("col_text_a".to_owned(), Value::Nullable(None)),
        ("col_text_b".to_owned(), Value::Nullable(None)),
        ("col_float".to_owned(), Value::Nullable(None)),
        ("col_int".to_owned(), Value::Nullable(None)),
        ("col_json".to_owned(), Value::Nullable(None)),
        ("col_tags".to_owned(), Value::Nullable(None)),
    ])
}

fn resource_access_test_cells(resource: RowUuid, team: RowUuid, administrator: bool) -> RowCells {
    BTreeMap::from([
        ("resource".to_owned(), Value::Uuid(resource.0)),
        ("team".to_owned(), Value::Uuid(team.0)),
        ("grant_role".to_owned(), Value::String("viewer".to_owned())),
        ("administrator".to_owned(), Value::Bool(administrator)),
    ])
}

fn group_access_test_cells(group: RowUuid, user: AuthorId) -> RowCells {
    BTreeMap::from([
        ("group_id".to_owned(), Value::Uuid(group.0)),
        ("user_id".to_owned(), Value::Uuid(user.0)),
        ("role".to_owned(), Value::String("viewer".to_owned())),
    ])
}

fn uuid_string_grant_role_schema(role: uuid::Uuid) -> JazzSchema {
    let resource_policy = Policy::shape(
        Query::from("docs")
            .reachable_via_with_access_filters(
                "doc_access_edges",
                "resource_id",
                "team_id",
                lit("relation-seeded"),
                [in_list(col("grant_role"), [lit(Value::Uuid(role))])],
                "team_entry",
                "member_id",
                "target_id",
                [],
            )
            .seeded_by("teams", "identity_key", "sub", "id"),
    );
    let access_branch = PolicyBranch::single_alternative_from_query(
        Query::from("doc_access_edges")
            .reachable_via(
                "doc_access_edges",
                "id",
                "team_id",
                lit("relation-seeded"),
                "team_entry",
                "member_id",
                "target_id",
                [],
            )
            .seeded_by("teams", "identity_key", "sub", "id"),
    );
    let mut access_query = Query::from("doc_access_edges");
    access_query.filters = vec![Predicate::Any(Vec::new())];
    access_query.policy_branches = vec![access_branch];
    let access_policy = Policy::shape(access_query);

    JazzSchema::new([
        TableSchema::new(
            "teams",
            [
                ColumnSchema::new("name", ColumnType::String),
                ColumnSchema::new("identity_key", ColumnType::Uuid),
            ],
        )
        .with_read_policy(Policy::public())
        .with_write_policy(Policy::public()),
        TableSchema::new(
            "team_entry",
            [
                ColumnSchema::new("member_id", ColumnType::Uuid),
                ColumnSchema::new("target_id", ColumnType::Uuid),
            ],
        )
        .with_reference("member_id", "teams")
        .with_reference("target_id", "teams")
        .with_read_policy(Policy::public())
        .with_write_policy(Policy::public()),
        TableSchema::new("docs", [ColumnSchema::new("title", ColumnType::String)])
            .with_read_policy(resource_policy)
            .with_write_policy(Policy::public()),
        TableSchema::new(
            "doc_access_edges",
            [
                ColumnSchema::new("resource_id", ColumnType::Uuid),
                ColumnSchema::new("team_id", ColumnType::Uuid),
                ColumnSchema::new("grant_role", ColumnType::String),
            ],
        )
        .with_reference("resource_id", "docs")
        .with_reference("team_id", "teams")
        .with_read_policy(access_policy)
        .with_write_policy(Policy::public()),
    ])
}

#[test]
fn string_grant_role_access_filter_matches_uuid_literal_in_list() {
    let role = uuid::Uuid::parse_str("0cae56e7-0f54-421c-ba8b-54fcbfec8dd2").unwrap();
    let schema = uuid_string_grant_role_schema(role);
    let server = open_core(0x6d, AuthorId::SYSTEM, &schema);
    let member = AuthorId::from_bytes([0x6e; 16]);
    let member_team = row(0x61);
    let resource_team = row(0x62);
    let doc = row(0x63);

    server
        .insert_with_id(
            "teams",
            member_team,
            BTreeMap::from([
                ("name".to_owned(), Value::String("member".to_owned())),
                ("identity_key".to_owned(), Value::Uuid(member.0)),
            ]),
        )
        .unwrap();
    server
        .insert_with_id(
            "teams",
            resource_team,
            BTreeMap::from([
                ("name".to_owned(), Value::String("resource".to_owned())),
                ("identity_key".to_owned(), Value::Uuid(row(0x64).0)),
            ]),
        )
        .unwrap();
    server
        .insert_with_id(
            "team_entry",
            row(0x65),
            BTreeMap::from([
                ("member_id".to_owned(), Value::Uuid(member_team.0)),
                ("target_id".to_owned(), Value::Uuid(resource_team.0)),
            ]),
        )
        .unwrap();
    server
        .insert_with_id(
            "docs",
            doc,
            BTreeMap::from([("title".to_owned(), Value::String("visible".to_owned()))]),
        )
        .unwrap();
    server
        .insert_with_id(
            "doc_access_edges",
            row(0x66),
            BTreeMap::from([
                ("resource_id".to_owned(), Value::Uuid(doc.0)),
                ("team_id".to_owned(), Value::Uuid(resource_team.0)),
                ("grant_role".to_owned(), Value::String(role.to_string())),
            ]),
        )
        .unwrap();

    assert_eq!(
        served_subscription_rows_for_author(&schema, &server, member, "docs"),
        vec![doc]
    );

    let db = block_on(Db::open_history_complete(DbConfig {
        schema: schema.clone(),
        storage: rocks_storage(&schema),
        identity: DbIdentity {
            node: NodeUuid::from_bytes([0x6f; 16]),
            author: AuthorId::SYSTEM,
        },
        id_source: Some(Box::new(SeededRowIdSource::new(0x6f))),
        large_value_checkpoint_op_interval: crate::node::LARGE_VALUE_CHECKPOINT_OP_INTERVAL,
    }))
    .unwrap();
    for (table, row_id, cells) in [
        (
            "teams",
            member_team,
            BTreeMap::from([
                ("name".to_owned(), Value::String("member".to_owned())),
                ("identity_key".to_owned(), Value::Uuid(member.0)),
            ]),
        ),
        (
            "teams",
            resource_team,
            BTreeMap::from([
                ("name".to_owned(), Value::String("resource".to_owned())),
                ("identity_key".to_owned(), Value::Uuid(row(0x64).0)),
            ]),
        ),
        (
            "team_entry",
            row(0x65),
            BTreeMap::from([
                ("member_id".to_owned(), Value::Uuid(member_team.0)),
                ("target_id".to_owned(), Value::Uuid(resource_team.0)),
            ]),
        ),
        (
            "docs",
            doc,
            BTreeMap::from([("title".to_owned(), Value::String("visible".to_owned()))]),
        ),
        (
            "doc_access_edges",
            row(0x66),
            BTreeMap::from([
                ("resource_id".to_owned(), Value::Uuid(doc.0)),
                ("team_id".to_owned(), Value::Uuid(resource_team.0)),
                ("grant_role".to_owned(), Value::String(role.to_string())),
            ]),
        ),
    ] {
        db.seed_settled_mergeable_for_bootstrap(table, row_id, AuthorId::SYSTEM, cells)
            .unwrap();
    }
    let prepared = db.prepare_query(&Query::from("docs")).unwrap();
    let one_shot = block_on(db.all_for_identity(
        &prepared,
        ReadOpts {
            tier: DurabilityTier::Global,
            ..ReadOpts::default()
        },
        member,
    ))
    .unwrap();
    assert_eq!(row_ids(&one_shot), vec![doc]);

    let access = db.prepare_query(&Query::from("doc_access_edges")).unwrap();
    let access_rows = block_on(db.all_for_identity(
        &access,
        ReadOpts {
            tier: DurabilityTier::Global,
            ..ReadOpts::default()
        },
        member,
    ))
    .unwrap();
    assert_eq!(row_ids(&access_rows), vec![row(0x66)]);
}

#[test]
fn customer_resource_access_edge_policy_requires_group_access_seed() {
    let schema = customer_resource_policy_minimal_schema();
    let server = open_core(0x5e, AuthorId::SYSTEM, &schema);
    let member = AuthorId::from_bytes([0x11; 16]);
    let group = row(0x22);
    let resource = row(0xd1);

    server
        .insert_with_id(
            "org",
            row(0x01),
            BTreeMap::from([("label".to_owned(), Value::String("org".to_owned()))]),
        )
        .unwrap();
    server
        .insert_with_id("group", group, team_cells("member-group"))
        .unwrap();
    server
        .insert_with_id(
            "group_access_edges",
            row(0xa1),
            group_access_test_cells(group, member),
        )
        .unwrap();
    server
        .insert_with_id("res_i", resource, resource_test_cells("visible"))
        .unwrap();
    server
        .insert_with_id(
            "res_i_access_edges",
            row(0xb1),
            resource_access_test_cells(resource, group, false),
        )
        .unwrap();
    assert_eq!(
        served_subscription_rows_for_author(&schema, &server, member, "res_i"),
        vec![resource]
    );
}

#[test]
fn seeded_membership_resource_policy_allows_direct_and_transitive_groups() {
    let schema = customer_resource_policy_minimal_schema();
    let server = open_core(0x5f, AuthorId::SYSTEM, &schema);
    let member = AuthorId::from_bytes([0x12; 16]);
    let other = AuthorId::from_bytes([0x13; 16]);
    let (direct, transitive, hidden) =
        seed_seeded_membership_resource_fixture(&server, member, other);

    assert_eq!(
        served_subscription_rows_for_author(&schema, &server, member, "res_i"),
        vec![direct, transitive]
    );
    assert_eq!(
        served_subscription_rows_for_author(&schema, &server, other, "res_i"),
        vec![hidden]
    );
    assert!(
        served_subscription_rows_for_author(
            &schema,
            &server,
            AuthorId::from_bytes([0x99; 16]),
            "res_i"
        )
        .is_empty()
    );
}

#[test]
fn direct_multi_identity_subscribe_reuses_shared_seeded_fragments_without_leaking() {
    let schema = customer_resource_policy_minimal_schema();
    let db = open_db(0x69, AuthorId::SYSTEM, &schema);
    let member = AuthorId::from_bytes([0x12; 16]);
    let other = AuthorId::from_bytes([0x13; 16]);
    let spy = AuthorId::from_bytes([0x99; 16]);
    db.insert_with_id(
        "org",
        row(0x01),
        BTreeMap::from([("label".to_owned(), Value::String("org".to_owned()))]),
    )
    .unwrap();
    let direct_group = row(0x31);
    let transitive_group = row(0x32);
    let hidden_group = row(0x33);
    let direct = row(0xd1);
    let transitive = row(0xd2);
    let hidden = row(0xd3);
    for (group, name) in [
        (direct_group, "direct"),
        (transitive_group, "transitive"),
        (hidden_group, "hidden"),
    ] {
        db.insert_with_id("group", group, team_cells(name)).unwrap();
    }
    db.insert_with_id(
        "group_access_edges",
        row(0xa1),
        group_access_test_cells(direct_group, member),
    )
    .unwrap();
    db.insert_with_id(
        "group_access_edges",
        row(0xa2),
        group_access_test_cells(hidden_group, other),
    )
    .unwrap();
    db.insert_with_id(
        "group_entry",
        row(0xc1),
        group_entry_test_cells(direct_group, transitive_group, false),
    )
    .unwrap();
    for (resource, title) in [
        (direct, "direct"),
        (transitive, "transitive"),
        (hidden, "hidden"),
    ] {
        db.insert_with_id("res_i", resource, resource_test_cells(title))
            .unwrap();
    }
    for (edge, resource, group) in [
        (row(0xb1), direct, direct_group),
        (row(0xb2), transitive, transitive_group),
        (row(0xb3), hidden, hidden_group),
    ] {
        db.insert_with_id(
            "res_i_access_edges",
            edge,
            resource_access_test_cells(resource, group, false),
        )
        .unwrap();
    }
    let prepared = db.prepare_query(&Query::from("res_i")).unwrap();
    let opts = ReadOpts::default();

    db.node.node.borrow().reset_storage_read_metrics();
    let mut member_subscription =
        block_on(db.subscribe_for_identity(&prepared, opts.clone(), member)).unwrap();
    assert_eq!(
        row_ids(&opened_rows(
            block_on(member_subscription.next_event()).unwrap()
        )),
        vec![direct, transitive]
    );
    let member_reads = db.node.node.borrow().take_storage_read_metrics();
    assert!(
        member_reads.total.reads > 0,
        "first identity should hydrate the shared seeded fragments"
    );

    db.node.node.borrow().reset_storage_read_metrics();
    let mut other_subscription =
        block_on(db.subscribe_for_identity(&prepared, opts.clone(), other)).unwrap();
    assert_eq!(
        row_ids(&opened_rows(
            block_on(other_subscription.next_event()).unwrap()
        )),
        vec![hidden]
    );
    let other_reads = db.node.node.borrow().take_storage_read_metrics();

    db.node.node.borrow().reset_storage_read_metrics();
    let mut spy_subscription = block_on(db.subscribe_for_identity(&prepared, opts, spy)).unwrap();
    assert!(opened_rows(block_on(spy_subscription.next_event()).unwrap()).is_empty());
    let spy_reads = db.node.node.borrow().take_storage_read_metrics();

    assert!(
        other_reads.total.reads < member_reads.total.reads,
        "second identity should probe shared hydrated fragments, not rescan them: first={:?}, second={:?}",
        member_reads,
        other_reads
    );
    assert!(
        spy_reads.total.reads < member_reads.total.reads,
        "zero-grant identity should also reuse shared canonical fragments without seeing rows: first={:?}, spy={:?}",
        member_reads,
        spy_reads
    );
}

#[test]
fn direct_same_identity_subscribe_reuses_shared_seeded_fragments_across_shapes() {
    let schema = customer_two_resource_policy_minimal_schema();
    let db = open_db(0x6a, AuthorId::SYSTEM, &schema);
    let member = AuthorId::from_bytes([0x12; 16]);
    db.insert_with_id(
        "org",
        row(0x01),
        BTreeMap::from([("label".to_owned(), Value::String("org".to_owned()))]),
    )
    .unwrap();
    let direct_group = row(0x31);
    let transitive_group = row(0x32);
    for (group, name) in [(direct_group, "direct"), (transitive_group, "transitive")] {
        db.insert_with_id("group", group, team_cells(name)).unwrap();
    }
    db.insert_with_id(
        "group_access_edges",
        row(0xa1),
        group_access_test_cells(direct_group, member),
    )
    .unwrap();
    db.insert_with_id(
        "group_entry",
        row(0xc1),
        group_entry_test_cells(direct_group, transitive_group, false),
    )
    .unwrap();

    let res_i_direct = row(0xd1);
    let res_i_transitive = row(0xd2);
    let res_j_direct = row(0xe1);
    let res_j_transitive = row(0xe2);
    for (table, resource, title) in [
        ("res_i", res_i_direct, "i-direct"),
        ("res_i", res_i_transitive, "i-transitive"),
        ("res_j", res_j_direct, "j-direct"),
        ("res_j", res_j_transitive, "j-transitive"),
    ] {
        db.insert_with_id(table, resource, resource_test_cells(title))
            .unwrap();
    }
    for (table, edge, resource, group) in [
        ("res_i_access_edges", row(0xb1), res_i_direct, direct_group),
        (
            "res_i_access_edges",
            row(0xb2),
            res_i_transitive,
            transitive_group,
        ),
        ("res_j_access_edges", row(0xb3), res_j_direct, direct_group),
        (
            "res_j_access_edges",
            row(0xb4),
            res_j_transitive,
            transitive_group,
        ),
    ] {
        db.insert_with_id(
            table,
            edge,
            resource_access_test_cells(resource, group, false),
        )
        .unwrap();
    }

    let res_i = db.prepare_query(&Query::from("res_i")).unwrap();
    let res_j = db.prepare_query(&Query::from("res_j")).unwrap();
    let opts = ReadOpts::default();

    db.node.node.borrow().reset_storage_read_metrics();
    let mut first = block_on(db.subscribe_for_identity(&res_i, opts.clone(), member)).unwrap();
    assert_eq!(
        row_ids(&opened_rows(block_on(first.next_event()).unwrap())),
        vec![res_i_direct, res_i_transitive]
    );
    let first_reads = db.node.node.borrow().take_storage_read_metrics();

    db.node.node.borrow().reset_storage_read_metrics();
    let mut second = block_on(db.subscribe_for_identity(&res_j, opts, member)).unwrap();
    assert_eq!(
        row_ids(&opened_rows(block_on(second.next_event()).unwrap())),
        vec![res_j_direct, res_j_transitive]
    );
    let second_reads = db.node.node.borrow().take_storage_read_metrics();

    assert!(
        second_reads.total.reads < first_reads.total.reads,
        "second shape should probe shared hydrated fragments, not rescan them: first={:?}, second={:?}",
        first_reads,
        second_reads
    );
}

#[test]
fn seeded_membership_grant_and_revoke_propagate_incrementally() {
    let schema = customer_resource_policy_minimal_schema();
    let server = open_core(0x60, AuthorId::SYSTEM, &schema);
    let member = AuthorId::from_bytes([0x14; 16]);
    let group = row(0x41);
    let resource = row(0xd4);
    let access = row(0xb4);

    seed_customer_resource_base(&server);
    server
        .insert_with_id("group", group, team_cells("direct"))
        .unwrap();
    server
        .insert_with_id("res_i", resource, resource_test_cells("resource"))
        .unwrap();
    server
        .insert_with_id(
            "res_i_access_edges",
            access,
            resource_access_test_cells(resource, group, false),
        )
        .unwrap();

    let client = open_db(0x61, member, &schema);
    let (client_transport, server_transport) = duplex();
    let _upstream = client.connect_upstream(client_transport);
    let _subscriber = server.accept_subscriber(server_transport, member);
    let mut subscription =
        prepared_subscribe(&client, &Query::from("res_i"), ReadOpts::default()).unwrap();
    assert!(opened_rows(block_on(subscription.next_event()).unwrap()).is_empty());

    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();
    while let Some(event) = subscription.try_next_event() {
        if let SubscriptionEvent::Delta {
            added,
            updated,
            removed,
            ..
        } = event
        {
            assert!(added.is_empty());
            assert!(updated.is_empty());
            assert!(removed.is_empty());
        }
    }

    server
        .insert_with_id(
            "group_access_edges",
            row(0xa4),
            group_access_test_cells(group, member),
        )
        .unwrap();
    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();
    let (added, updated, removed) = delta_rows(block_on(subscription.next_event()).unwrap());
    assert_eq!(row_ids(&added), vec![resource]);
    assert!(updated.is_empty());
    assert!(removed.is_empty());

    server
        .update(
            "res_i_access_edges",
            access,
            BTreeMap::from([("administrator".to_owned(), Value::Bool(true))]),
        )
        .unwrap();
    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();
    let (added, updated, removed) = delta_rows(block_on(subscription.next_event()).unwrap());
    assert!(added.is_empty());
    assert!(updated.is_empty());
    assert_eq!(
        removed
            .into_iter()
            .map(|row| row.row_uuid)
            .collect::<Vec<_>>(),
        vec![resource]
    );
}

#[test]
fn same_table_seeded_membership_allows_direct_and_transitive_groups() {
    let schema = same_table_seeded_resource_policy_schema();
    let server = open_core(0x66, AuthorId::SYSTEM, &schema);
    let member = AuthorId::from_bytes([0x21; 16]);
    let other = AuthorId::from_bytes([0x22; 16]);
    let (direct, transitive, hidden) =
        seed_same_table_seeded_resource_fixture(&server, member, other);

    assert_eq!(
        served_subscription_rows_for_author(&schema, &server, member, "resources"),
        vec![direct, transitive]
    );
    assert_eq!(
        served_subscription_rows_for_author(&schema, &server, other, "resources"),
        vec![hidden]
    );
    assert!(
        served_subscription_rows_for_author(
            &schema,
            &server,
            AuthorId::from_bytes([0x99; 16]),
            "resources"
        )
        .is_empty()
    );
}

#[test]
fn same_table_string_seeded_membership_allows_direct_and_transitive_groups() {
    let schema = same_table_string_seeded_resource_policy_schema();
    let server = open_core(0x86, AuthorId::SYSTEM, &schema);
    let member = AuthorId::from_bytes([0x21; 16]);
    let other = AuthorId::from_bytes([0x22; 16]);
    let (direct, transitive, hidden) =
        seed_same_table_string_seeded_resource_fixture(&server, member, other);

    assert_eq!(
        served_subscription_rows_for_author(&schema, &server, member, "resources"),
        vec![direct, transitive]
    );
    assert_eq!(
        served_subscription_rows_for_author(&schema, &server, other, "resources"),
        vec![hidden]
    );
    assert!(
        served_subscription_rows_for_author(
            &schema,
            &server,
            AuthorId::from_bytes([0x99; 16]),
            "resources"
        )
        .is_empty()
    );
}

#[test]
fn same_table_seeded_membership_identity_key_update_propagates_incrementally() {
    let schema = same_table_seeded_resource_policy_schema();
    let server = open_core(0x67, AuthorId::SYSTEM, &schema);
    let member = AuthorId::from_bytes([0x23; 16]);
    let other = AuthorId::from_bytes([0x24; 16]);
    let direct_group = row(0x71);
    let transitive_group = row(0x72);
    let resource = row(0xe7);

    for (group, identity, label) in [
        (direct_group, other, "direct"),
        (transitive_group, other, "transitive"),
    ] {
        server
            .insert_with_id("teams", group, same_table_team_cells(label, identity))
            .unwrap();
    }
    server
        .insert_with_id(
            "team_entries",
            row(0xc7),
            same_table_team_entry_cells(direct_group, transitive_group, false),
        )
        .unwrap();
    server
        .insert_with_id("resources", resource, same_table_resource_cells("resource"))
        .unwrap();
    server
        .insert_with_id(
            "resource_access",
            row(0xb7),
            same_table_resource_access_cells(resource, transitive_group, false),
        )
        .unwrap();

    let client = open_db(0x68, member, &schema);
    let (client_transport, server_transport) = duplex();
    let _upstream = client.connect_upstream(client_transport);
    let _subscriber = server.accept_subscriber(server_transport, member);
    let mut subscription =
        prepared_subscribe(&client, &Query::from("resources"), ReadOpts::default()).unwrap();
    assert!(opened_rows(block_on(subscription.next_event()).unwrap()).is_empty());

    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();
    while let Some(event) = subscription.try_next_event() {
        let (added, updated, removed) = match event {
            SubscriptionEvent::Delta {
                added,
                updated,
                removed,
                ..
            } => (added, updated, removed),
            SubscriptionEvent::Rejected { reason } => {
                panic!("unexpected subscription rejection: {reason:?}")
            }
            SubscriptionEvent::Closed => (Vec::new(), Vec::new(), Vec::new()),
        };
        assert!(added.is_empty());
        assert!(updated.is_empty());
        assert!(removed.is_empty());
    }

    server
        .update(
            "teams",
            direct_group,
            BTreeMap::from([("identity_key".to_owned(), Value::Uuid(member.0))]),
        )
        .unwrap();
    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();
    let (added, updated, removed) = delta_rows(block_on(subscription.next_event()).unwrap());
    assert_eq!(row_ids(&added), vec![resource]);
    assert!(updated.is_empty());
    assert!(removed.is_empty());

    server
        .update(
            "teams",
            direct_group,
            BTreeMap::from([("identity_key".to_owned(), Value::Uuid(other.0))]),
        )
        .unwrap();
    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();
    let (added, updated, removed) = delta_rows(block_on(subscription.next_event()).unwrap());
    assert!(added.is_empty());
    assert!(updated.is_empty());
    assert_eq!(
        removed
            .into_iter()
            .map(|row| row.row_uuid)
            .collect::<Vec<_>>(),
        vec![resource]
    );
}

#[test]
fn inherited_child_policy_allows_two_and_three_level_chains_per_identity() {
    let schema = customer_inherited_child_policy_schema();
    let server = open_core(0x62, AuthorId::SYSTEM, &schema);
    let member = AuthorId::from_bytes([0x15; 16]);
    let other = AuthorId::from_bytes([0x16; 16]);
    let (member_child, member_grandchild, other_child, other_grandchild) =
        seed_inherited_child_fixture(&server, member, other);

    assert_eq!(
        served_subscription_rows_for_author(&schema, &server, member, "res_i_child"),
        vec![member_child]
    );
    assert_eq!(
        served_subscription_rows_for_author(&schema, &server, member, "res_i_grandchild"),
        vec![member_grandchild]
    );
    assert_eq!(
        served_subscription_rows_for_author(&schema, &server, other, "res_i_child"),
        vec![other_child]
    );
    assert_eq!(
        served_subscription_rows_for_author(&schema, &server, other, "res_i_grandchild"),
        vec![other_grandchild]
    );
    let spy = AuthorId::from_bytes([0x99; 16]);
    assert!(served_subscription_rows_for_author(&schema, &server, spy, "res_i_child").is_empty());
    assert!(
        served_subscription_rows_for_author(&schema, &server, spy, "res_i_grandchild").is_empty()
    );
}

#[test]
fn inherited_child_policy_parent_revocation_propagates_incrementally() {
    let schema = customer_inherited_child_policy_schema();
    let server = open_core(0x63, AuthorId::SYSTEM, &schema);
    let member = AuthorId::from_bytes([0x17; 16]);
    let other = AuthorId::from_bytes([0x18; 16]);
    let (child, _grandchild, _other_child, _other_grandchild) =
        seed_inherited_child_fixture(&server, member, other);

    let client = open_db(0x64, member, &schema);
    let (client_transport, server_transport) = duplex();
    let _upstream = client.connect_upstream(client_transport);
    let _subscriber = server.accept_subscriber(server_transport, member);
    let mut subscription =
        prepared_subscribe(&client, &Query::from("res_i_child"), ReadOpts::default()).unwrap();
    assert!(opened_rows(block_on(subscription.next_event()).unwrap()).is_empty());

    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();
    let (added, updated, removed) = delta_rows(block_on(subscription.next_event()).unwrap());
    assert_eq!(row_ids(&added), vec![child]);
    assert!(updated.is_empty());
    assert!(removed.is_empty());

    server
        .update(
            "res_i_access_edges",
            row(0xbb),
            BTreeMap::from([("administrator".to_owned(), Value::Bool(true))]),
        )
        .unwrap();
    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();
    let (added, updated, removed) = delta_rows(block_on(subscription.next_event()).unwrap());
    assert!(added.is_empty());
    assert!(updated.is_empty());
    assert_eq!(
        removed
            .into_iter()
            .map(|row| row.row_uuid)
            .collect::<Vec<_>>(),
        vec![child]
    );
}

#[test]
fn inherited_child_policy_composes_with_local_predicates() {
    let schema = customer_inherited_child_policy_schema();
    let server = open_core(0x65, AuthorId::SYSTEM, &schema);
    let member = AuthorId::from_bytes([0x19; 16]);
    let other = AuthorId::from_bytes([0x1a; 16]);
    let (open_child, _grandchild, _other_child, _other_grandchild) =
        seed_inherited_child_fixture(&server, member, other);
    let closed_child = row(0xee);
    server
        .insert_with_id(
            "res_i_child",
            closed_child,
            child_cells(row(0xdd), "closed", "closed child"),
        )
        .unwrap();

    assert_eq!(
        served_subscription_rows_for_author(&schema, &server, member, "res_i_child"),
        vec![open_child]
    );
}

#[test]
fn inherited_child_insert_uses_parent_update_where_old_only() {
    let schema = inherited_insert_policy_schema();
    let member = AuthorId::from_bytes([0x21; 16]);
    let other = AuthorId::from_bytes([0x22; 16]);
    let member_db = open_db(0x66, member, &schema);
    let parent = row(0xf1);
    member_db
        .insert_with_id(
            "parents",
            parent,
            BTreeMap::from([
                ("owner".to_owned(), Value::Uuid(member.0)),
                ("locked".to_owned(), Value::Bool(true)),
            ]),
        )
        .unwrap();

    member_db
        .insert_with_id("children", row(0xf2), child_insert_cells(parent, "allowed"))
        .unwrap();

    let other_db = open_db(0x67, other, &schema);
    other_db
        .insert_with_id(
            "parents",
            parent,
            BTreeMap::from([
                ("owner".to_owned(), Value::Uuid(member.0)),
                ("locked".to_owned(), Value::Bool(true)),
            ]),
        )
        .unwrap();
    let err = match other_db.insert_with_id(
        "children",
        row(0xf3),
        child_insert_cells(parent, "denied"),
    ) {
        Ok(_) => panic!("child insert should be rejected when parent update_using denies"),
        Err(err) => err,
    };
    assert_eq!(err.code, ErrorCode::WriteRejected);
}

fn seed_customer_resource_base(server: &CoreDb) {
    server
        .insert_with_id(
            "org",
            row(0x01),
            BTreeMap::from([("label".to_owned(), Value::String("org".to_owned()))]),
        )
        .unwrap();
}

fn seed_seeded_membership_resource_fixture(
    server: &CoreDb,
    member: AuthorId,
    other: AuthorId,
) -> (RowUuid, RowUuid, RowUuid) {
    seed_customer_resource_base(server);
    let direct_group = row(0x31);
    let transitive_group = row(0x32);
    let hidden_group = row(0x33);
    let direct = row(0xd1);
    let transitive = row(0xd2);
    let hidden = row(0xd3);

    for (group, name) in [
        (direct_group, "direct"),
        (transitive_group, "transitive"),
        (hidden_group, "hidden"),
    ] {
        server
            .insert_with_id("group", group, team_cells(name))
            .unwrap();
    }
    server
        .insert_with_id(
            "group_access_edges",
            row(0xa1),
            group_access_test_cells(direct_group, member),
        )
        .unwrap();
    server
        .insert_with_id(
            "group_access_edges",
            row(0xa2),
            group_access_test_cells(hidden_group, other),
        )
        .unwrap();
    server
        .insert_with_id(
            "group_entry",
            row(0xc1),
            group_entry_test_cells(direct_group, transitive_group, false),
        )
        .unwrap();
    for (resource, title) in [
        (direct, "direct"),
        (transitive, "transitive"),
        (hidden, "hidden"),
    ] {
        server
            .insert_with_id("res_i", resource, resource_test_cells(title))
            .unwrap();
    }
    for (edge, resource, group) in [
        (row(0xb1), direct, direct_group),
        (row(0xb2), transitive, transitive_group),
        (row(0xb3), hidden, hidden_group),
    ] {
        server
            .insert_with_id(
                "res_i_access_edges",
                edge,
                resource_access_test_cells(resource, group, false),
            )
            .unwrap();
    }
    (direct, transitive, hidden)
}

fn seed_same_table_seeded_resource_fixture(
    server: &CoreDb,
    member: AuthorId,
    other: AuthorId,
) -> (RowUuid, RowUuid, RowUuid) {
    let direct_group = row(0x61);
    let transitive_group = row(0x62);
    let hidden_group = row(0x63);
    let direct = row(0xf1);
    let transitive = row(0xf2);
    let hidden = row(0xf3);

    for (group, identity, label) in [
        (direct_group, member, "direct"),
        (
            transitive_group,
            AuthorId::from_bytes([0x88; 16]),
            "transitive",
        ),
        (hidden_group, other, "hidden"),
    ] {
        server
            .insert_with_id("teams", group, same_table_team_cells(label, identity))
            .unwrap();
    }
    server
        .insert_with_id(
            "team_entries",
            row(0xc6),
            same_table_team_entry_cells(direct_group, transitive_group, false),
        )
        .unwrap();
    for (resource, label) in [
        (direct, "direct"),
        (transitive, "transitive"),
        (hidden, "hidden"),
    ] {
        server
            .insert_with_id("resources", resource, same_table_resource_cells(label))
            .unwrap();
    }
    for (edge, resource, group) in [
        (row(0xb6), direct, direct_group),
        (row(0xb7), transitive, transitive_group),
        (row(0xb8), hidden, hidden_group),
    ] {
        server
            .insert_with_id(
                "resource_access",
                edge,
                same_table_resource_access_cells(resource, group, false),
            )
            .unwrap();
    }
    (direct, transitive, hidden)
}

fn seed_same_table_string_seeded_resource_fixture(
    server: &CoreDb,
    member: AuthorId,
    other: AuthorId,
) -> (RowUuid, RowUuid, RowUuid) {
    let direct_group = row(0x61);
    let transitive_group = row(0x62);
    let hidden_group = row(0x63);
    let direct = row(0xf1);
    let transitive = row(0xf2);
    let hidden = row(0xf3);

    for (group, identity, label) in [
        (direct_group, member.0.to_string(), "direct"),
        (transitive_group, "not-the-member".to_owned(), "transitive"),
        (hidden_group, other.0.to_string(), "hidden"),
    ] {
        server
            .insert_with_id(
                "teams",
                group,
                same_table_team_string_cells(label, &identity),
            )
            .unwrap();
    }
    server
        .insert_with_id(
            "team_entries",
            row(0xc6),
            same_table_team_entry_cells(direct_group, transitive_group, false),
        )
        .unwrap();
    for (resource, label) in [
        (direct, "direct"),
        (transitive, "transitive"),
        (hidden, "hidden"),
    ] {
        server
            .insert_with_id("resources", resource, same_table_resource_cells(label))
            .unwrap();
    }
    for (edge, resource, group) in [
        (row(0xb6), direct, direct_group),
        (row(0xb7), transitive, transitive_group),
        (row(0xb8), hidden, hidden_group),
    ] {
        server
            .insert_with_id(
                "resource_access",
                edge,
                same_table_resource_access_cells(resource, group, false),
            )
            .unwrap();
    }
    (direct, transitive, hidden)
}

fn seed_inherited_child_fixture(
    server: &CoreDb,
    member: AuthorId,
    other: AuthorId,
) -> (RowUuid, RowUuid, RowUuid, RowUuid) {
    seed_customer_resource_base(server);
    let member_group = row(0xd1);
    let other_group = row(0xd2);
    let member_resource = row(0xdd);
    let other_resource = row(0xde);
    let member_child = row(0xe1);
    let other_child = row(0xe2);
    let member_grandchild = row(0xe3);
    let other_grandchild = row(0xe4);

    for (group, label) in [(member_group, "member"), (other_group, "other")] {
        server
            .insert_with_id("group", group, team_cells(label))
            .unwrap();
    }
    server
        .insert_with_id(
            "group_access_edges",
            row(0xaa),
            group_access_test_cells(member_group, member),
        )
        .unwrap();
    server
        .insert_with_id(
            "group_access_edges",
            row(0xab),
            group_access_test_cells(other_group, other),
        )
        .unwrap();
    for (resource, group, label) in [
        (member_resource, member_group, "member-resource"),
        (other_resource, other_group, "other-resource"),
    ] {
        server
            .insert_with_id(
                "res_i",
                resource,
                resource_test_cells_with_group(label, group),
            )
            .unwrap();
    }
    server
        .insert_with_id(
            "res_i_access_edges",
            row(0xbb),
            resource_access_test_cells(member_resource, member_group, false),
        )
        .unwrap();
    server
        .insert_with_id(
            "res_i_access_edges",
            row(0xbc),
            resource_access_test_cells(other_resource, other_group, false),
        )
        .unwrap();
    server
        .insert_with_id(
            "res_i_child",
            member_child,
            child_cells(member_resource, "open", "member-child"),
        )
        .unwrap();
    server
        .insert_with_id(
            "res_i_child",
            other_child,
            child_cells(other_resource, "open", "other-child"),
        )
        .unwrap();
    server
        .insert_with_id(
            "res_i_grandchild",
            member_grandchild,
            grandchild_cells(member_child, "member-grandchild"),
        )
        .unwrap();
    server
        .insert_with_id(
            "res_i_grandchild",
            other_grandchild,
            grandchild_cells(other_child, "other-grandchild"),
        )
        .unwrap();
    (
        member_child,
        member_grandchild,
        other_child,
        other_grandchild,
    )
}

fn team_cells(name: &str) -> RowCells {
    BTreeMap::from([("name".to_owned(), Value::String(name.to_owned()))])
}

fn same_table_team_cells(name: &str, identity: AuthorId) -> RowCells {
    BTreeMap::from([
        ("name".to_owned(), Value::String(name.to_owned())),
        ("identity_key".to_owned(), Value::Uuid(identity.0)),
    ])
}

fn same_table_team_string_cells(name: &str, identity: &str) -> RowCells {
    BTreeMap::from([
        ("name".to_owned(), Value::String(name.to_owned())),
        (
            "identity_key".to_owned(),
            Value::String(identity.to_owned()),
        ),
    ])
}

fn group_entry_test_cells(member: RowUuid, target: RowUuid, administrator: bool) -> RowCells {
    BTreeMap::from([
        ("member_id".to_owned(), Value::Uuid(member.0)),
        ("target_id".to_owned(), Value::Uuid(target.0)),
        ("administrator".to_owned(), Value::Bool(administrator)),
        ("date_added".to_owned(), Value::U64(1)),
    ])
}

fn same_table_team_entry_cells(member: RowUuid, target: RowUuid, administrator: bool) -> RowCells {
    BTreeMap::from([
        ("member_id".to_owned(), Value::Uuid(member.0)),
        ("target_id".to_owned(), Value::Uuid(target.0)),
        ("administrator".to_owned(), Value::Bool(administrator)),
    ])
}

fn same_table_resource_cells(label: &str) -> RowCells {
    BTreeMap::from([("label".to_owned(), Value::String(label.to_owned()))])
}

fn same_table_resource_access_cells(
    resource: RowUuid,
    group: RowUuid,
    administrator: bool,
) -> RowCells {
    BTreeMap::from([
        ("resource".to_owned(), Value::Uuid(resource.0)),
        ("team".to_owned(), Value::Uuid(group.0)),
        ("administrator".to_owned(), Value::Bool(administrator)),
    ])
}

fn child_cells(resource: RowUuid, status: &str, label: &str) -> RowCells {
    BTreeMap::from([
        ("resource".to_owned(), Value::Uuid(resource.0)),
        ("status".to_owned(), Value::String(status.to_owned())),
        ("label".to_owned(), Value::String(label.to_owned())),
    ])
}

fn grandchild_cells(child: RowUuid, label: &str) -> RowCells {
    BTreeMap::from([
        ("child".to_owned(), Value::Uuid(child.0)),
        ("label".to_owned(), Value::String(label.to_owned())),
    ])
}

fn child_insert_cells(parent: RowUuid, label: &str) -> RowCells {
    BTreeMap::from([
        ("parent_id".to_owned(), Value::Uuid(parent.0)),
        ("label".to_owned(), Value::String(label.to_owned())),
    ])
}

fn seed_recursive_reachable_read_fixture(server: &CoreDb, member: AuthorId) -> (RowUuid, RowUuid) {
    let direct_doc = row(0xd1);
    let inherited_doc = row(0xd2);
    let hidden_doc = row(0xd3);
    let member_team = RowUuid(member.0);
    let parent_team = row(0xa1);
    let hidden_team = row(0xa2);

    for (team, name) in [
        (member_team, "member"),
        (parent_team, "parent"),
        (hidden_team, "hidden"),
    ] {
        server
            .insert_with_id("group", team, team_cells(name))
            .unwrap();
    }

    for (doc, title) in [
        (direct_doc, "direct"),
        (inherited_doc, "inherited"),
        (hidden_doc, "hidden"),
    ] {
        server
            .insert_with_id("res_a", doc, resource_test_cells(title))
            .unwrap();
    }

    server
        .insert_with_id(
            "res_a_access_edges",
            row(0xb1),
            resource_access_test_cells(direct_doc, member_team, false),
        )
        .unwrap();
    server
        .insert_with_id(
            "group_access_edges",
            row(0xa1),
            group_access_test_cells(member_team, member),
        )
        .unwrap();
    server
        .insert_with_id(
            "res_a_access_edges",
            row(0xb2),
            resource_access_test_cells(inherited_doc, parent_team, false),
        )
        .unwrap();
    server
        .insert_with_id(
            "res_a_access_edges",
            row(0xb3),
            resource_access_test_cells(hidden_doc, hidden_team, false),
        )
        .unwrap();
    for i in 0..42 {
        let member = if i == 0 { member_team } else { parent_team };
        let target = parent_team;
        server
            .insert_with_id(
                "group_entry",
                row(0xc1 + i),
                group_entry_test_cells(member, target, false),
            )
            .unwrap();
    }

    (direct_doc, inherited_doc)
}

fn served_subscription_rows_for_author(
    schema: &JazzSchema,
    server: &CoreDb,
    author: AuthorId,
    table: &str,
) -> Vec<RowUuid> {
    let client = open_db(author.0.as_bytes()[0], author, schema);
    let (client_transport, server_transport) = duplex();
    let _upstream = client.connect_upstream(client_transport);
    let _subscriber = server.accept_subscriber(server_transport, author);
    let query = Query::from(table);
    let mut subscription = prepared_subscribe(&client, &query, ReadOpts::default()).unwrap();
    assert!(opened_rows(block_on(subscription.next_event()).unwrap()).is_empty());
    let mut rows = BTreeSet::new();

    for _ in 0..8 {
        client.tick().unwrap();
        server.tick().unwrap();
        client.tick().unwrap();
        while let Some(event) = subscription.try_next_event() {
            if let SubscriptionEvent::Delta {
                reset,
                added,
                updated,
                removed,
                ..
            } = event
            {
                if reset {
                    rows.clear();
                }
                for row in removed {
                    rows.remove(&row.row_uuid);
                }
                for row in added.into_iter().chain(updated) {
                    rows.insert(row.row_uuid());
                }
            }
        }
    }
    rows.into_iter().collect()
}

fn served_many_subscription_rows_for_author(
    schema: &JazzSchema,
    server: &CoreDb,
    author: AuthorId,
    tables: &[&str],
) -> BTreeMap<String, Vec<RowUuid>> {
    let client = open_db(author.0.as_bytes()[0].wrapping_add(0x40), author, schema);
    let (client_transport, server_transport) = duplex();
    let _upstream = client.connect_upstream(client_transport);
    let _subscriber = server.accept_subscriber(server_transport, author);
    let mut subscriptions = Vec::new();
    for table in tables {
        let query = Query::from(*table);
        let mut subscription = prepared_subscribe(&client, &query, ReadOpts::default()).unwrap();
        assert!(opened_rows(block_on(subscription.next_event()).unwrap()).is_empty());
        subscriptions.push(((*table).to_owned(), subscription));
    }

    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();

    subscriptions
        .into_iter()
        .map(|(table, mut subscription)| {
            let (added, updated, removed) =
                delta_rows(block_on(subscription.next_event()).unwrap());
            assert!(updated.is_empty());
            assert!(removed.is_empty());
            (table, row_ids(&added))
        })
        .collect()
}

fn served_group_entry_rows_via_relay(
    schema: &JazzSchema,
    server: &CoreDb,
    author: AuthorId,
) -> (Vec<RowUuid>, usize, usize) {
    let relay = open_db(0x71, AuthorId::SYSTEM, schema);
    let client = open_db(0x72, author, schema);
    let (relay_transport, core_transport) = duplex();
    let _relay_upstream = relay.connect_upstream(relay_transport);
    let _core_subscriber = server.accept_subscriber(core_transport, AuthorId::SYSTEM);
    let (client_transport, relay_sub_transport) = duplex();
    let _client_upstream = client.connect_upstream(client_transport);
    let _relay_subscriber = relay.accept_subscriber(relay_sub_transport, author);

    let query = Query::from("group_entry");
    let mut subscription = prepared_subscribe(&client, &query, ReadOpts::default()).unwrap();
    assert!(opened_rows(block_on(subscription.next_event()).unwrap()).is_empty());
    let mut rows = BTreeSet::new();
    for _ in 0..20 {
        server.server.tick().unwrap();
        relay.tick().unwrap();
        client.tick().unwrap();
        while let Some(event) = subscription.try_next_event() {
            if let SubscriptionEvent::Delta {
                reset,
                added,
                updated,
                removed,
                ..
            } = event
            {
                if reset {
                    rows.clear();
                }
                for row in removed {
                    rows.remove(&row.row_uuid);
                }
                for row in added.into_iter().chain(updated) {
                    rows.insert(row.row_uuid());
                }
            }
        }
    }
    let client_query = client.prepare_query(&Query::from("group_entry")).unwrap();
    let client_one_shot = block_on(client.all(&client_query, ReadOpts::default()))
        .unwrap()
        .len();
    let relay_query = relay.prepare_query(&Query::from("group_entry")).unwrap();
    let relay_one_shot = block_on(relay.all(&relay_query, ReadOpts::default()))
        .unwrap()
        .len();
    (rows.into_iter().collect(), client_one_shot, relay_one_shot)
}

#[test]
fn db_surface_recursive_reachable_claim_policy_subscription_routes_per_identity() {
    let schema = benchmark_shaped_recursive_reachable_read_schema();
    let server = open_core(0x5e, AuthorId::SYSTEM, &schema);
    let member = AuthorId::from_bytes([0x11; 16]);
    let admin = AuthorId::SYSTEM;
    let spy = AuthorId::from_bytes([0x33; 16]);
    let (direct_doc, inherited_doc) = seed_recursive_reachable_read_fixture(&server, member);

    assert_eq!(
        served_subscription_rows_for_author(&schema, &server, member, "res_a"),
        vec![direct_doc, inherited_doc]
    );
    assert_eq!(
        served_subscription_rows_for_author(&schema, &server, admin, "res_a"),
        vec![direct_doc, inherited_doc, row(0xd3)]
    );
    assert!(served_subscription_rows_for_author(&schema, &server, spy, "res_a").is_empty());
    assert_eq!(
        served_subscription_rows_for_author(&schema, &server, member, "group_entry"),
        (0..42).map(|i| row(0xc1 + i)).collect::<Vec<_>>()
    );
    let rows = served_many_subscription_rows_for_author(
        &schema,
        &server,
        member,
        &["group", "res_a_access_edges", "res_a", "group_entry"],
    );
    assert_eq!(
        rows["group_entry"],
        (0..42).map(|i| row(0xc1 + i)).collect::<Vec<_>>()
    );
    let (relay_rows, client_one_shot, relay_one_shot) =
        served_group_entry_rows_via_relay(&schema, &server, member);
    assert_eq!(relay_one_shot, 42);
    assert_eq!(client_one_shot, 42);
    assert_eq!(
        relay_rows,
        (0..42).map(|i| row(0xc1 + i)).collect::<Vec<_>>()
    );
}

#[test]
fn db_sync_surface_uploads_client_writes_for_authority_fate() {
    let schema = schema();
    let author = AuthorId::from_bytes([0xc1; 16]);
    let server = open_core(0x5e, AuthorId::SYSTEM, &schema);
    let client = open_db(0xc1, author, &schema);

    let (client_transport, server_transport) = duplex();
    let _upstream = client.connect_upstream(client_transport);
    let _subscriber = server.accept_subscriber(server_transport, author);

    // A local client write is Local and queued for upload.
    let write = client
        .insert("todos", cells("from client", false, author))
        .unwrap();
    let row = write.row_uuid();

    // Drive: client uploads the commit unit -> server (authority) accepts to
    // Global and sends the fate back -> client applies the fate.
    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();

    // The client's own write reached Global once the authority fate landed.
    assert_eq!(
        block_on(write.wait(DurabilityTier::Global)).unwrap(),
        write.mergeable_tx_id()
    );
    // The authority received and applied the uploaded row.
    let server_rows = server.read(&Query::from("todos")).unwrap();
    assert_eq!(server_rows.len(), 1);
    assert_eq!(server_rows[0].row_uuid(), row);
}

#[test]
fn byte_wire_uploads_client_writes_for_authority_fate() {
    let schema = schema();
    let author = AuthorId::from_bytes([0xc1; 16]);
    let server = open_core(0x5e, AuthorId::SYSTEM, &schema);
    let client = open_db(0xc1, author, &schema);

    let (client_transport, server_transport) = byte_duplex();
    let _upstream = client.connect_upstream(client_transport);
    let _subscriber = server.accept_subscriber(server_transport, author);

    let write = client
        .insert("todos", cells("from client", false, author))
        .unwrap();
    let row = write.row_uuid();

    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();

    assert_eq!(
        block_on(write.wait(DurabilityTier::Global)).unwrap(),
        write.mergeable_tx_id()
    );
    let server_rows = server.read(&Query::from("todos")).unwrap();
    assert_eq!(server_rows.len(), 1);
    assert_eq!(server_rows[0].row_uuid(), row);
}

#[test]
fn db_sync_surface_uploads_client_exclusive_commit_for_global_fate() {
    let schema = schema();
    let author = AuthorId::from_bytes([0xc1; 16]);
    let server = open_core(0x5e, AuthorId::SYSTEM, &schema);
    let client = open_db(0xc1, author, &schema);

    let (client_transport, server_transport) = duplex();
    let _upstream = client.connect_upstream(client_transport);
    let _subscriber = server.accept_subscriber(server_transport, author);

    let row = row(0xe1);
    let exclusive = client.exclusive_tx().unwrap();
    exclusive
        .insert_with_id("todos", row, cells("exclusive", false, author))
        .unwrap();
    let tx_id = exclusive.commit().unwrap();

    assert_eq!(
        client.write_state(tx_id).unwrap(),
        WriteState {
            fate: Fate::Pending,
            durability: DurabilityTier::Local,
        }
    );

    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();

    assert_eq!(
        client.write_state(tx_id).unwrap(),
        WriteState {
            fate: Fate::Accepted,
            durability: DurabilityTier::Global,
        }
    );
    let server_rows = server.read(&Query::from("todos")).unwrap();
    assert_eq!(server_rows.len(), 1);
    assert_eq!(server_rows[0].row_uuid(), row);
}

#[test]
fn db_sync_surface_returns_exclusive_conflict_fate_to_client() {
    let schema = schema();
    let author = AuthorId::from_bytes([0xc1; 16]);
    let server = open_core(0x5e, AuthorId::SYSTEM, &schema);
    let client = open_db(0xc1, author, &schema);

    let (client_transport, server_transport) = duplex();
    let _upstream = client.connect_upstream(client_transport);
    let _subscriber = server.accept_subscriber(server_transport, author);

    let row = row(0xe2);
    let first = client.exclusive_tx().unwrap();
    let second = client.exclusive_tx().unwrap();
    first
        .insert_with_id("todos", row, cells("first", false, author))
        .unwrap();
    second
        .insert_with_id("todos", row, cells("second", false, author))
        .unwrap();
    let first_tx = first.commit().unwrap();
    let second_error = second.commit().unwrap_err();
    assert_eq!(second_error.code, ErrorCode::TransactionConflict);
    assert!(second_error.message.contains("visible parent changed"));

    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();

    assert_eq!(
        client.write_state(first_tx).unwrap(),
        WriteState {
            fate: Fate::Accepted,
            durability: DurabilityTier::Global,
        }
    );
    let rows = server.read(&Query::from("todos")).unwrap();
    assert_eq!(rows.len(), 1);
    let table = &schema.tables[0];
    assert_eq!(
        rows[0].cell(table, "title"),
        Some(Value::String("first".to_owned()))
    );
}

#[test]
fn write_fate_and_durability_are_queryable_through_facade() {
    let schema = schema();
    let author = AuthorId::from_bytes([0xc1; 16]);
    let server = open_core(0x5e, AuthorId::SYSTEM, &schema);
    let client = open_db(0xc1, author, &schema);

    let (client_transport, server_transport) = duplex();
    let _upstream = client.connect_upstream(client_transport);
    let _subscriber = server.accept_subscriber(server_transport, author);

    let write = client
        .insert("todos", cells("facade state", false, author))
        .unwrap();
    assert_eq!(
        write.write_state().unwrap(),
        WriteState {
            fate: Fate::Pending,
            durability: DurabilityTier::Local,
        }
    );
    assert_eq!(
        client.write_state(write.mergeable_tx_id()).unwrap(),
        write.write_state().unwrap()
    );

    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();

    assert_eq!(
        write.write_state().unwrap(),
        WriteState {
            fate: Fate::Accepted,
            durability: DurabilityTier::Global,
        }
    );
    assert_eq!(
        block_on(write.wait(DurabilityTier::Global)).unwrap(),
        write.mergeable_tx_id()
    );
}

#[test]
fn session_upload_rejects_forged_made_by_without_ingesting_rows() {
    let schema = owner_write_schema();
    let session_author = AuthorId::from_bytes([0xc1; 16]);
    let forged_author = AuthorId::from_bytes([0xa1; 16]);
    let server = open_core(0x5e, AuthorId::SYSTEM, &schema);
    let client = open_db(0xc1, session_author, &schema);

    let (client_transport, server_transport) = duplex();
    let _upstream = client.connect_upstream(client_transport);
    let _subscriber = server.accept_subscriber(server_transport, session_author);

    let tx_id = client
        .node
        .node
        .borrow_mut()
        .commit_mergeable(
            MergeableCommit::new("todos", row(0xf1), client.next_now_ms())
                .made_by(forged_author)
                .cells(cells("forged", false, session_author)),
        )
        .unwrap();
    client
        .node
        .outbox
        .borrow_mut()
        .push(PendingUpload { tx_id, unit: None });

    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();

    let handle = WriteHandle {
        node: Rc::downgrade(&client.node.node),
        row_uuid: row(0xf1),
        tx_id,
        local_tier: DurabilityTier::Local,
    };
    let err = block_on(handle.wait(DurabilityTier::Global)).unwrap_err();
    assert_eq!(err.code, ErrorCode::WriteRejected);
    assert!(server.read(&Query::from("todos")).unwrap().is_empty());
}

#[test]
fn session_upload_uses_connection_identity_for_write_policy() {
    let schema = owner_write_schema();
    let session_author = AuthorId::from_bytes([0xc1; 16]);
    let server = open_core(0x5e, AuthorId::SYSTEM, &schema);
    let client = open_db(0xc1, session_author, &schema);

    let (client_transport, server_transport) = duplex();
    let _upstream = client.connect_upstream(client_transport);
    let _subscriber = server.accept_subscriber(server_transport, session_author);

    let write = client
        .insert("todos", cells("honest", false, session_author))
        .unwrap();
    let row = write.row_uuid();

    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();

    assert_eq!(
        block_on(write.wait(DurabilityTier::Global)).unwrap(),
        write.mergeable_tx_id()
    );
    let rows = server.read(&Query::from("todos")).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].row_uuid(), row);
}

#[test]
fn session_delete_uses_current_row_for_owner_write_policy() {
    let schema = owner_write_schema();
    let session_author = AuthorId::from_bytes([0xc1; 16]);
    let other_author = AuthorId::from_bytes([0xd1; 16]);
    let server = open_core(0x5e, AuthorId::SYSTEM, &schema);
    let client = open_db(0xc1, session_author, &schema);

    let (client_transport, server_transport) = duplex();
    let _upstream = client.connect_upstream(client_transport);
    let _subscriber = server.accept_subscriber(server_transport, session_author);

    let write = client
        .insert("todos", cells("owned", false, session_author))
        .unwrap();
    let row = write.row_uuid();
    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();
    block_on(write.wait(DurabilityTier::Global)).unwrap();

    let bad_delete = match client.delete_for_identity(other_author, "todos", row) {
        Ok(_) => panic!("foreign owner delete should be rejected locally"),
        Err(error) => error,
    };
    assert_eq!(bad_delete.code, ErrorCode::WriteRejected);

    let delete = client
        .delete_for_identity(session_author, "todos", row)
        .unwrap();
    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();

    assert_eq!(
        block_on(delete.wait(DurabilityTier::Global)).unwrap(),
        delete.mergeable_tx_id()
    );
    assert!(server.read(&Query::from("todos")).unwrap().is_empty());
}

#[test]
fn trusted_backend_upload_uses_backend_policy_and_stores_user_made_by() {
    let schema = owner_write_schema();
    let backend_author = AuthorId::from_bytes([0xb0; 16]);
    let attributed_user = AuthorId::from_bytes([0xa1; 16]);
    let server = open_core(0x5e, AuthorId::SYSTEM, &schema);
    let backend = open_db(0xb0, backend_author, &schema);

    let (backend_transport, server_transport) = duplex();
    let _upstream = backend.connect_upstream(backend_transport);
    let _subscriber = server.accept_subscriber_with_trust(
        server_transport,
        backend_author,
        CommitUnitTrust::TrustedBackend,
    );

    let tx_id = backend
        .node
        .node
        .borrow_mut()
        .commit_mergeable(
            MergeableCommit::new("todos", row(0xf2), backend.next_now_ms())
                .made_by(attributed_user)
                .permission_subject(backend_author)
                .cells(cells("attributed", false, backend_author)),
        )
        .unwrap();
    backend
        .node
        .outbox
        .borrow_mut()
        .push(PendingUpload { tx_id, unit: None });

    backend.tick().unwrap();
    server.tick().unwrap();
    backend.tick().unwrap();

    let SyncMessage::CommitUnit { tx, .. } =
        server.node().borrow_mut().commit_unit_for(tx_id).unwrap()
    else {
        panic!("expected stored commit unit");
    };
    assert_eq!(tx.made_by, attributed_user);
    let rows = server.read(&Query::from("todos")).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].row_uuid(), row(0xf2));
}

#[test]
fn trusted_backend_upload_applies_session_claim_assertions_for_write_policy() {
    let schema = editor_claim_write_schema();
    let backend_author = AuthorId::from_bytes([0xb0; 16]);
    let editor_author = AuthorId::from_bytes([0xe1; 16]);
    let server = open_core(0x5e, AuthorId::SYSTEM, &schema);
    let backend = open_db(0xb0, backend_author, &schema);

    let (backend_transport, server_transport) = duplex();
    let _upstream = backend.connect_upstream(backend_transport);
    let _subscriber = server.accept_subscriber_with_trust(
        server_transport,
        backend_author,
        CommitUnitTrust::TrustedBackend,
    );

    backend.set_identity_claims(
        editor_author,
        BTreeMap::from([("role".to_owned(), Value::String("editor".to_owned()))]),
    );
    let write = backend
        .insert_with_id_for_identity(
            editor_author,
            "todos",
            row(0xe1),
            cells("claim-backed", false, editor_author),
        )
        .unwrap();

    backend.tick().unwrap();
    server.tick().unwrap();
    backend.tick().unwrap();

    assert_eq!(
        block_on(write.wait(DurabilityTier::Global)).unwrap(),
        write.mergeable_tx_id()
    );
    let rows = server.read(&Query::from("todos")).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].row_uuid(), row(0xe1));
}

#[test]
fn session_claim_assertions_require_trusted_backend_upload() {
    let schema = editor_claim_write_schema();
    let session_author = AuthorId::from_bytes([0xe1; 16]);
    let server = open_core(0x5e, AuthorId::SYSTEM, &schema);
    let client = open_db(0xe1, session_author, &schema);

    let (client_transport, server_transport) = duplex();
    let _upstream = client.connect_upstream(client_transport);
    let _subscriber = server.accept_subscriber(server_transport, session_author);

    client.set_identity_claims(
        session_author,
        BTreeMap::from([("role".to_owned(), Value::String("editor".to_owned()))]),
    );
    let write = client
        .insert_with_id_for_identity(
            session_author,
            "todos",
            row(0xe2),
            cells("claim-backed", false, session_author),
        )
        .unwrap();

    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();

    let err = block_on(write.wait(DurabilityTier::Global)).unwrap_err();
    assert_eq!(err.code, ErrorCode::WriteRejected);
    assert!(server.read(&Query::from("todos")).unwrap().is_empty());
}

#[test]
fn trusted_backend_delete_uses_permission_subject_parent_for_write_policy() {
    let schema = owner_write_schema();
    let backend_author = AuthorId::from_bytes([0xb0; 16]);
    let attributed_user = AuthorId::from_bytes([0xa1; 16]);
    let server = open_core(0x5e, AuthorId::SYSTEM, &schema);
    let backend = open_db(0xb0, backend_author, &schema);

    let (backend_transport, server_transport) = duplex();
    let _upstream = backend.connect_upstream(backend_transport);
    let _subscriber = server.accept_subscriber_with_trust(
        server_transport,
        backend_author,
        CommitUnitTrust::TrustedBackend,
    );

    let insert = backend
        .insert_with_id_for_identity(
            attributed_user,
            "todos",
            row(0xf3),
            cells("attributed", false, attributed_user),
        )
        .unwrap();
    backend.tick().unwrap();
    server.tick().unwrap();
    backend.tick().unwrap();
    block_on(insert.wait(DurabilityTier::Global)).unwrap();

    let delete = backend
        .delete_for_identity(attributed_user, "todos", row(0xf3))
        .unwrap();
    backend.tick().unwrap();
    server.tick().unwrap();
    backend.tick().unwrap();

    assert_eq!(
        block_on(delete.wait(DurabilityTier::Global)).unwrap(),
        delete.mergeable_tx_id()
    );
    assert!(server.read(&Query::from("todos")).unwrap().is_empty());
}

#[test]
fn db_large_text_values_round_trip_across_edit_chain() {
    let schema =
        JazzSchema::new([
            TableSchema::new("notes", [crate::schema::ColumnSchema::text("body")])
                .with_read_policy(Policy::public())
                .with_write_policy(Policy::public()),
        ]);
    let dir = tempfile::tempdir().unwrap();
    let cfs = schema.column_families();
    let refs = cfs.iter().map(String::as_str).collect::<Vec<_>>();
    let storage = RocksDbStorage::open(dir.path(), &refs).unwrap();
    let db = block_on(Db::open(DbConfig {
        schema: schema.clone(),
        storage,
        identity: DbIdentity {
            node: NodeUuid::from_bytes([0x33; 16]),
            author: AuthorId::from_bytes([0x44; 16]),
        },
        id_source: Some(Box::new(SeededRowIdSource::new(0x33))),
        large_value_checkpoint_op_interval: crate::node::LARGE_VALUE_CHECKPOINT_OP_INTERVAL,
    }))
    .unwrap();
    let table = &schema.tables[0];

    let write = db
        .insert(
            "notes",
            BTreeMap::from([("body".to_owned(), Value::Bytes(b"hello".to_vec()))]),
        )
        .unwrap();
    let note = write.row_uuid();
    assert_eq!(
        prepared_large_value_cell(&db, &Query::from("notes"), table, "body"),
        b"hello".to_vec()
    );

    for value in [
        "hello world".as_bytes().to_vec(),
        "hello brave world".as_bytes().to_vec(),
        "brave new world".as_bytes().to_vec(),
        "brave new world - ecriture 日本".as_bytes().to_vec(),
    ] {
        db.update(
            "notes",
            note,
            BTreeMap::from([("body".to_owned(), Value::Bytes(value.clone()))]),
        )
        .unwrap();
        assert_eq!(
            prepared_large_value_cell(&db, &Query::from("notes"), table, "body"),
            value
        );
    }
}

#[test]
fn db_large_blob_values_round_trip_binary_from_empty_parent() {
    let schema =
        JazzSchema::new([
            TableSchema::new("files", [crate::schema::ColumnSchema::blob("data")])
                .with_read_policy(Policy::public())
                .with_write_policy(Policy::public()),
        ]);
    let dir = tempfile::tempdir().unwrap();
    let cfs = schema.column_families();
    let refs = cfs.iter().map(String::as_str).collect::<Vec<_>>();
    let storage = RocksDbStorage::open(dir.path(), &refs).unwrap();
    let db = block_on(Db::open(DbConfig {
        schema: schema.clone(),
        storage,
        identity: DbIdentity {
            node: NodeUuid::from_bytes([0x55; 16]),
            author: AuthorId::from_bytes([0x66; 16]),
        },
        id_source: Some(Box::new(SeededRowIdSource::new(0x55))),
        large_value_checkpoint_op_interval: crate::node::LARGE_VALUE_CHECKPOINT_OP_INTERVAL,
    }))
    .unwrap();
    let table = &schema.tables[0];
    let first = vec![0, 1, 2, 3, 255, 0, 128];
    let second = vec![0, 1, 9, 3, 255, 64, 128, 200];

    let write = db
        .insert(
            "files",
            BTreeMap::from([("data".to_owned(), Value::Bytes(first.clone()))]),
        )
        .unwrap();
    let file = write.row_uuid();
    assert_eq!(
        prepared_large_value_cell(&db, &Query::from("files"), table, "data"),
        first
    );

    db.update(
        "files",
        file,
        BTreeMap::from([("data".to_owned(), Value::Bytes(second.clone()))]),
    )
    .unwrap();
    assert_eq!(
        prepared_large_value_cell(&db, &Query::from("files"), table, "data"),
        second
    );
}

#[test]
fn db_text_edit_ops_materialize_expected_value() {
    let schema =
        JazzSchema::new([
            TableSchema::new("notes", [crate::schema::ColumnSchema::text("body")])
                .with_read_policy(Policy::public())
                .with_write_policy(Policy::public()),
        ]);
    let dir = tempfile::tempdir().unwrap();
    let cfs = schema.column_families();
    let refs = cfs.iter().map(String::as_str).collect::<Vec<_>>();
    let storage = RocksDbStorage::open(dir.path(), &refs).unwrap();
    let db = block_on(Db::open(DbConfig {
        schema: schema.clone(),
        storage,
        identity: DbIdentity {
            node: NodeUuid::from_bytes([0x77; 16]),
            author: AuthorId::from_bytes([0x88; 16]),
        },
        id_source: Some(Box::new(SeededRowIdSource::new(0x77))),
        large_value_checkpoint_op_interval: crate::node::LARGE_VALUE_CHECKPOINT_OP_INTERVAL,
    }))
    .unwrap();
    let table = &schema.tables[0];
    let write = db
        .insert(
            "notes",
            BTreeMap::from([("body".to_owned(), Value::Bytes(b"hello world".to_vec()))]),
        )
        .unwrap();

    db.edit_text(
        "notes",
        write.row_uuid(),
        "body",
        TextEdit::new().delete(5, 6).insert(5, b", ops".to_vec()),
    )
    .unwrap();

    assert_eq!(
        prepared_large_value_cell(&db, &Query::from("notes"), table, "body"),
        b"hello, ops".to_vec()
    );
}

#[test]
fn db_text_dump_and_edit_paths_interleave() {
    let schema =
        JazzSchema::new([
            TableSchema::new("notes", [crate::schema::ColumnSchema::text("body")])
                .with_read_policy(Policy::public())
                .with_write_policy(Policy::public()),
        ]);
    let dir = tempfile::tempdir().unwrap();
    let cfs = schema.column_families();
    let refs = cfs.iter().map(String::as_str).collect::<Vec<_>>();
    let storage = RocksDbStorage::open(dir.path(), &refs).unwrap();
    let db = block_on(Db::open(DbConfig {
        schema: schema.clone(),
        storage,
        identity: DbIdentity {
            node: NodeUuid::from_bytes([0x78; 16]),
            author: AuthorId::from_bytes([0x89; 16]),
        },
        id_source: Some(Box::new(SeededRowIdSource::new(0x78))),
        large_value_checkpoint_op_interval: crate::node::LARGE_VALUE_CHECKPOINT_OP_INTERVAL,
    }))
    .unwrap();
    let table = &schema.tables[0];
    let write = db
        .insert(
            "notes",
            BTreeMap::from([("body".to_owned(), Value::Bytes(b"start".to_vec()))]),
        )
        .unwrap();
    let row = write.row_uuid();

    db.update(
        "notes",
        row,
        BTreeMap::from([("body".to_owned(), Value::Bytes(b"start middle".to_vec()))]),
    )
    .unwrap();
    db.edit_text(
        "notes",
        row,
        "body",
        TextEdit::new().insert(12, b" end".to_vec()),
    )
    .unwrap();
    db.update(
        "notes",
        row,
        BTreeMap::from([(
            "body".to_owned(),
            Value::Bytes(b"BEGIN middle end".to_vec()),
        )]),
    )
    .unwrap();
    db.edit_text("notes", row, "body", TextEdit::new().delete(5, 7))
        .unwrap();

    assert_eq!(
        prepared_large_value_cell(&db, &Query::from("notes"), table, "body"),
        b"BEGIN end".to_vec()
    );
}

#[test]
fn db_blob_edit_ops_handle_binary_and_multibyte_bytes() {
    let schema =
        JazzSchema::new([
            TableSchema::new("files", [crate::schema::ColumnSchema::blob("data")])
                .with_read_policy(Policy::public())
                .with_write_policy(Policy::public()),
        ]);
    let dir = tempfile::tempdir().unwrap();
    let cfs = schema.column_families();
    let refs = cfs.iter().map(String::as_str).collect::<Vec<_>>();
    let storage = RocksDbStorage::open(dir.path(), &refs).unwrap();
    let db = block_on(Db::open(DbConfig {
        schema: schema.clone(),
        storage,
        identity: DbIdentity {
            node: NodeUuid::from_bytes([0x79; 16]),
            author: AuthorId::from_bytes([0x8a; 16]),
        },
        id_source: Some(Box::new(SeededRowIdSource::new(0x79))),
        large_value_checkpoint_op_interval: crate::node::LARGE_VALUE_CHECKPOINT_OP_INTERVAL,
    }))
    .unwrap();
    let table = &schema.tables[0];
    let write = db
        .insert(
            "files",
            BTreeMap::from([("data".to_owned(), Value::Bytes("aé日z".as_bytes().to_vec()))]),
        )
        .unwrap();

    db.edit_text(
        "files",
        write.row_uuid(),
        "data",
        TextEdit::new()
            .delete(1, "é".len())
            .insert(6, vec![0, 255])
            .insert(7, "✓".as_bytes().to_vec()),
    )
    .unwrap();

    let mut expected = Vec::new();
    expected.extend_from_slice(b"a");
    expected.extend_from_slice("日".as_bytes());
    expected.extend_from_slice(&[0, 255]);
    expected.extend_from_slice(b"z");
    expected.extend_from_slice("✓".as_bytes());
    assert_eq!(
        prepared_large_value_cell(&db, &Query::from("files"), table, "data"),
        expected
    );
}

#[test]
fn db_query_builder_expresses_s1_shaped_filters_and_include_modes() {
    let schema = issue_schema();
    let dir = tempfile::tempdir().unwrap();
    let cfs = schema.column_families();
    let refs = cfs.iter().map(String::as_str).collect::<Vec<_>>();
    let storage = RocksDbStorage::open(dir.path(), &refs).unwrap();
    let alice = AuthorId::from_bytes([0xa1; 16]);
    let bob = AuthorId::from_bytes([0xb2; 16]);
    let db = block_on(Db::open(DbConfig {
        schema: schema.clone(),
        storage,
        identity: DbIdentity {
            node: NodeUuid::from_bytes([0x22; 16]),
            author: alice,
        },
        id_source: Some(Box::new(SeededRowIdSource::new(0x22))),
        large_value_checkpoint_op_interval: crate::node::LARGE_VALUE_CHECKPOINT_OP_INTERVAL,
    }))
    .unwrap();

    db.insert_with_id(
        "projects",
        row(10),
        BTreeMap::from([("name".to_owned(), Value::String("Platform".to_owned()))]),
    )
    .unwrap();
    db.insert_with_id(
        "issues",
        row(1),
        issue_cells(
            "ship api query builder",
            "open",
            alice,
            row(10),
            5,
            &["api", "platform"],
            None,
        ),
    )
    .unwrap();
    db.insert_with_id(
        "issues",
        row(2),
        issue_cells("closed work", "done", alice, row(10), 3, &["api"], Some(99)),
    )
    .unwrap();
    db.insert_with_id(
        "issues",
        row(3),
        issue_cells("someone else", "open", bob, row(10), 8, &["platform"], None),
    )
    .unwrap();
    db.insert_with_id(
        "issues",
        row(4),
        issue_cells("missing project", "open", alice, row(99), 6, &["api"], None),
    )
    .unwrap();

    let s1_query = db
        .table("issues")
        .filter(all_of([
            eq(col("assignee"), lit(alice.0)),
            in_list(col("state"), [lit("open"), lit("blocked")]),
            not(ne(col("state"), lit("open"))),
            any_of([
                contains(col("title"), lit("api")),
                contains(col("labels"), lit("api")),
            ]),
            gt(col("priority"), lit(4_u64)),
            lte(col("priority"), lit(6_u64)),
            is_null(col("snoozed_until")),
        ]))
        .include("project")
        .select([
            "title", "state", "assignee", "project", "priority", "labels",
        ])
        .limit(10)
        .offset(0);

    let table = schema
        .tables
        .iter()
        .find(|table| table.name == "issues")
        .unwrap();
    let read_rows = prepared_read(&db, &s1_query);
    assert_eq!(row_ids(&read_rows), vec![row(1)]);
    assert_eq!(
        read_rows[0].cell(table, "title"),
        Some(Value::String("ship api query builder".to_owned()))
    );
    assert_eq!(read_rows[0].cell(table, "snoozed_until"), None);
    let all_rows = prepared_all(&db, &s1_query, ReadOpts::default());
    assert_eq!(row_ids(&all_rows), vec![row(1)]);

    let holes_query = db
        .table("issues")
        .filter(eq(col("assignee"), lit(alice.0)))
        .filter(eq(col("state"), lit("open")))
        .include_with(Include::new("project").join_mode(JoinMode::Holes));
    assert_eq!(
        row_ids(&prepared_read(&db, &holes_query)),
        vec![row(1), row(4)]
    );

    let require_query = holes_query.clone().include_with(
        Include::new("project")
            .join_mode(JoinMode::Holes)
            .require_includes(),
    );
    assert_eq!(row_ids(&prepared_read(&db, &require_query)), vec![row(1)]);
    assert_eq!(
        row_ids(&prepared_all(&db, &require_query, ReadOpts::default())),
        vec![row(1)],
        "required scalar includes must retain public Root membership gating"
    );

    let paged = db
        .table("issues")
        .filter(eq(col("state"), lit("open")))
        .include_with(Include::new("project").join_mode(JoinMode::Holes))
        .offset(1)
        .limit(1);
    assert_eq!(row_ids(&prepared_read(&db, &paged)), vec![row(3)]);
}

fn row_ids(rows: &[CurrentRow]) -> Vec<RowUuid> {
    rows.iter().map(CurrentRow::row_uuid).collect()
}

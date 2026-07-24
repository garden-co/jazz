use std::future::Future;
use std::pin::pin;
use std::task::{Context, Poll, Waker};

use groove::schema::{ColumnSchema, ColumnType};
use groove::storage::{OrderedKvStorage, ReopenableStorage, RocksDbStorage};

use super::*;
use crate::ids::{AuthorId, BranchId, NodeUuid};
use crate::protocol::{
    BindingViewKey, CatalogueAck, KnownStateCompleteness, KnownStateDeclaration, LensOp,
    ReadViewSourceSpec, ReadViewSpec, RegisterShapeOptions, ResultMemberEntry, RowVersionRef,
    ShapeAst, Subscribe, SubscribeRejectReason, TableLens,
};
use crate::protocol_limits::{
    MAX_CONTENT_EXTENT_BYTES, MAX_FETCH_ROW_VERSIONS, MAX_KNOWN_STATE_EXACT_REFS,
    MAX_SHAPE_AST_BYTES, MAX_SYNC_MESSAGE_BYTES, MAX_WIRE_FRAME_BYTES,
};
use crate::query::{
    ArraySubquery, BindingId, Include, JoinMode, OrderDirection, PolicyBranch, Predicate, ShapeId,
    all_of, any_of, claim, col, contains, eq, gt, in_list, is_null, lit, lte, ne, not,
};
use crate::schema::{Policy, TableSchema, WritePolicies};
use crate::time::{GlobalSeq, TxTime};
use crate::tx::TxId;
use crate::wire::decode_sync_message;
use crate::wire::{
    FEATURE_STRUCTURED_ERRORS, FEATURE_SYNC_MESSAGE_PAYLOAD, WireStreamDecoder,
    current_wire_features,
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
            added_related,
            added_edges,
            removed_edges,
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
                if let Some(position) = snapshot.rows.iter().position(|current| {
                    current.table() == row.table() && current.row_uuid() == row.row_uuid()
                }) {
                    snapshot.rows[position] = row;
                }
            }

            for row in added {
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

            for row in added_related {
                if snapshot
                    .rows
                    .iter()
                    .take(snapshot.root_count)
                    .any(|root| root.table() == row.table() && root.row_uuid() == row.row_uuid())
                {
                    continue;
                }
                if let Some(position) =
                    snapshot
                        .rows
                        .iter()
                        .skip(snapshot.root_count)
                        .position(|current| {
                            current.table() == row.table() && current.row_uuid() == row.row_uuid()
                        })
                {
                    snapshot.rows[snapshot.root_count + position] = row;
                } else {
                    snapshot.rows.push(row);
                }
            }

            snapshot
                .edges
                .retain(|edge| !removed_edges.iter().any(|removed| removed == edge));
            for edge in added_edges {
                if !snapshot.edges.iter().any(|current| current == &edge) {
                    snapshot.edges.push(edge);
                }
            }

            let mut index = snapshot.root_count;
            while index < snapshot.rows.len() {
                let row = &snapshot.rows[index];
                let still_referenced = snapshot.edges.iter().any(|edge| {
                    edge.target_table == row.table() && edge.target_row == row.row_uuid()
                });
                if still_referenced {
                    index += 1;
                } else {
                    snapshot.rows.remove(index);
                }
            }
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
        } => (added, updated, removed),
        other => panic!("expected subscription delta event, got {other:?}"),
    }
}

fn snapshot_edges(event: &SubscriptionEvent) -> BTreeSet<RelationEdge> {
    let event = event.clone();
    let mut snapshot = RelationSnapshot::default();
    apply_subscription_event(&mut snapshot, event);
    snapshot.edges.iter().cloned().collect()
}

fn snapshot_from_event(event: SubscriptionEvent) -> RelationSnapshot {
    let mut snapshot = RelationSnapshot::default();
    apply_subscription_event(&mut snapshot, event);
    snapshot
}

fn schema_table<'a>(schema: &'a JazzSchema, table: &str) -> &'a TableSchema {
    schema
        .tables
        .iter()
        .find(|candidate| candidate.name == table)
        .expect("test schema table should exist")
}

fn related_text_values(
    snapshot: &RelationSnapshot,
    schema: &JazzSchema,
    source_table: &str,
    source_row: RowUuid,
    relation: &str,
    target_table: &str,
    column: &str,
) -> Vec<String> {
    let table = schema_table(schema, target_table);
    snapshot
        .edges
        .iter()
        .filter(|edge| {
            edge.source_table == source_table
                && edge.source_row == source_row
                && edge.relation == relation
                && edge.target_table == target_table
        })
        .map(|edge| {
            snapshot
                .rows
                .iter()
                .find(|row| row.table() == target_table && row.row_uuid() == edge.target_row)
                .unwrap_or_else(|| panic!("missing target row for edge {edge:?}"))
        })
        .map(|row| match row.cell(table, column) {
            Some(Value::String(value)) => value,
            other => panic!("expected text cell {target_table}.{column}, got {other:?}"),
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

fn sorted_related_text_values(
    snapshot: &RelationSnapshot,
    schema: &JazzSchema,
    source_table: &str,
    source_row: RowUuid,
    relation: &str,
    target_table: &str,
    column: &str,
) -> Vec<String> {
    let mut values = related_text_values(
        snapshot,
        schema,
        source_table,
        source_row,
        relation,
        target_table,
        column,
    );
    values.sort();
    values
}

fn ordered_limited_related_text_values(
    snapshot: &RelationSnapshot,
    schema: &JazzSchema,
    source_table: &str,
    source_row: RowUuid,
    relation: &str,
    target_table: &str,
    column: &str,
    limit: usize,
) -> Vec<String> {
    let mut values = sorted_related_text_values(
        snapshot,
        schema,
        source_table,
        source_row,
        relation,
        target_table,
        column,
    );
    values.truncate(limit);
    values
}

fn event_settled(event: &SubscriptionEvent) -> bool {
    match event {
        SubscriptionEvent::Delta { settled, .. } => *settled,
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
    JazzSchema::new([TableSchema::new(
        "todos",
        [
            ColumnSchema::new("title", ColumnType::String),
            ColumnSchema::new("done", ColumnType::Bool),
        ],
    )
    .with_read_policy(Policy::shape(
        Query::from("todos").filter(eq(col("$createdBy"), claim("sub"))),
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
                ColumnSchema::new("org_id", ColumnType::Uuid),
            ],
        )
        .with_reference("org_id", "orgs")
        .with_read_policy(Policy::public())
        .with_write_policy(Policy::public()),
        TableSchema::new(
            "users",
            [
                ColumnSchema::new("name", ColumnType::String),
                ColumnSchema::new("team_id", ColumnType::Uuid),
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
fn can_insert_dry_run_uses_current_identity_without_writing() {
    let schema = owner_write_schema();
    let owner = AuthorId::from_bytes([0xa1; 16]);
    let other = AuthorId::from_bytes([0xb2; 16]);
    let owner_db = open_db(0xa1, owner, &schema);
    let other_db = open_db(0xb2, other, &schema);

    assert!(
        owner_db
            .can_insert("todos", cells("owned", false, owner))
            .unwrap()
    );
    assert!(
        !other_db
            .can_insert("todos", cells("owned", false, owner))
            .unwrap()
    );
    assert_eq!(prepared_read(&owner_db, &owner_db.table("todos")).len(), 0);
    assert_eq!(prepared_read(&other_db, &other_db.table("todos")).len(), 0);
}

#[test]
fn can_read_dry_run_uses_current_local_winner() {
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

    assert!(owner_db.can_read("todos", row).unwrap());
    assert!(!other_db.can_read("todos", row).unwrap());
}

#[test]
fn can_delete_dry_run_is_gated_by_write_policy_without_mutating() {
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

    assert!(owner_db.can_delete("todos", row).unwrap());
    assert!(!other_db.can_delete("todos", row).unwrap());
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

    assert_eq!(
        snapshot
            .rows
            .iter()
            .map(|row| (row.table().to_owned(), row.row_uuid()))
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([
            ("todos".to_owned(), row(0x11)),
            ("users".to_owned(), row(0xa1)),
        ])
    );
    assert_eq!(
        snapshot.edges.into_iter().collect::<BTreeSet<_>>(),
        BTreeSet::from([RelationEdge {
            source_table: "users".to_owned(),
            source_row: row(0xa1),
            relation: "todosViaOwner".to_owned(),
            target_table: "todos".to_owned(),
            target_row: row(0x11),
        }])
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
            ("org_id".to_owned(), Value::Uuid(row(0x01).0)),
        ]),
    )
    .unwrap();
    db.insert_with_id(
        "users",
        row(0x21),
        BTreeMap::from([
            ("name".to_owned(), Value::String("User A".to_owned())),
            ("team_id".to_owned(), Value::Uuid(row(0x11).0)),
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
            ("org_id".to_owned(), Value::Uuid(row(0x01).0)),
        ]),
    )
    .unwrap();
    db.insert_with_id(
        "users",
        row(0x21),
        BTreeMap::from([
            ("name".to_owned(), Value::String("User A".to_owned())),
            ("team_id".to_owned(), Value::Uuid(row(0x11).0)),
        ]),
    )
    .unwrap();

    let query = users_to_orgs_relation_query();

    let mut stream = block_on(db.subscribe_relation_query(&query, ReadOpts::default())).unwrap();
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
    assert_eq!(row_ids(&snapshot.rows), vec![row(0xa1), row(0x22)]);
    assert_eq!(snapshot.edges.len(), 1);
    assert_eq!(snapshot.edges[0].target_row, row(0x22));
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
    let message = client_transport
        .try_recv()
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
    let message = client_transport
        .try_recv()
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
    db.delete("todos", row(0x11)).unwrap();

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
    assert_eq!(row_ids(&snapshot.rows), vec![row(0xa1), row(0x22)]);
    assert_eq!(snapshot.edges.len(), 1);
    assert_eq!(snapshot.edges[0].target_row, row(0x22));
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

    assert_eq!(row_ids(&snapshot.rows), vec![user, todo]);
    assert_eq!(snapshot.edges.len(), 1);
    assert_eq!(snapshot.edges[0].source_row, user);
    assert_eq!(snapshot.edges[0].target_row, todo);
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
    let todo = db
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

    assert_eq!(row_ids(&snapshot.rows), vec![project, todo]);
    assert_eq!(snapshot.edges.len(), 1);
    assert_eq!(snapshot.edges[0].source_row, project);
    assert_eq!(snapshot.edges[0].target_row, todo);
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
    assert_eq!(row_ids(&snapshot.rows), vec![row(0xa1), row(0x22)]);
    assert_eq!(snapshot.edges.len(), 1);
    assert_eq!(snapshot.edges[0].target_row, row(0x22));
    let child = snapshot
        .rows
        .iter()
        .find(|candidate| candidate.row_uuid() == row(0x22))
        .expect("child row is materialized");
    let (descriptor, _) = child.encoded_record();
    assert!(descriptor.field_index("$createdAt").is_some());
    assert!(descriptor.field_index("$updatedAt").is_some());
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
fn array_subquery_live_subscription_tracks_child_edges() {
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
    assert_eq!(
        snapshot_edges(&opened),
        BTreeSet::new(),
        "initial parent has no children"
    );

    db.insert_with_id(
        "todos",
        row(0x11),
        BTreeMap::from([
            ("title".to_owned(), Value::String("first".to_owned())),
            ("owner_id".to_owned(), Value::Uuid(row(0xa1).0)),
        ]),
    )
    .unwrap();
    assert_eq!(
        snapshot_edges(&block_on(subscription.next_event()).unwrap()),
        BTreeSet::from([RelationEdge {
            source_table: "users".to_owned(),
            source_row: row(0xa1),
            relation: "todosViaOwner".to_owned(),
            target_table: "todos".to_owned(),
            target_row: row(0x11),
        }])
    );

    db.update(
        "todos",
        row(0x11),
        BTreeMap::from([("owner_id".to_owned(), Value::Uuid(row(0xb1).0))]),
    )
    .unwrap();
    assert_eq!(
        snapshot_edges(&block_on(subscription.next_event()).unwrap()),
        BTreeSet::new(),
        "child leaves the correlated array when its owner changes"
    );

    db.update(
        "todos",
        row(0x11),
        BTreeMap::from([("owner_id".to_owned(), Value::Uuid(row(0xa1).0))]),
    )
    .unwrap();
    assert_eq!(
        snapshot_edges(&block_on(subscription.next_event()).unwrap()),
        BTreeSet::from([RelationEdge {
            source_table: "users".to_owned(),
            source_row: row(0xa1),
            relation: "todosViaOwner".to_owned(),
            target_table: "todos".to_owned(),
            target_row: row(0x11),
        }]),
        "re-populated group is emitted as full current relation state"
    );
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

    let opened = snapshot_from_event(block_on(subscription.next_event()).unwrap());
    assert_eq!(
        sorted_related_text_values(
            &opened,
            &schema,
            "todos",
            row(0x21),
            "comments",
            "comments",
            "body"
        ),
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
    let after_insert = snapshot_from_event(block_on(subscription.next_event()).unwrap());
    assert_eq!(
        sorted_related_text_values(
            &after_insert,
            &schema,
            "todos",
            row(0x21),
            "comments",
            "comments",
            "body"
        ),
        vec!["first".to_owned()]
    );

    db.update(
        "comments",
        row(0xc1),
        BTreeMap::from([("body".to_owned(), Value::String("edited".to_owned()))]),
    )
    .unwrap();
    let after_update = snapshot_from_event(block_on(subscription.next_event()).unwrap());
    assert_eq!(
        sorted_related_text_values(
            &after_update,
            &schema,
            "todos",
            row(0x21),
            "comments",
            "comments",
            "body"
        ),
        vec!["edited".to_owned()]
    );

    db.delete("comments", row(0xc1)).unwrap();
    let after_child_delete = snapshot_from_event(block_on(subscription.next_event()).unwrap());
    assert_eq!(
        sorted_related_text_values(
            &after_child_delete,
            &schema,
            "todos",
            row(0x21),
            "comments",
            "comments",
            "body"
        ),
        Vec::<String>::new()
    );

    db.insert_with_id(
        "comments",
        row(0xc2),
        BTreeMap::from([
            ("body".to_owned(), Value::String("second".to_owned())),
            ("todo_id".to_owned(), Value::Uuid(row(0x21).0)),
        ]),
    )
    .unwrap();
    let after_repopulate = snapshot_from_event(block_on(subscription.next_event()).unwrap());
    assert_eq!(
        sorted_related_text_values(
            &after_repopulate,
            &schema,
            "todos",
            row(0x21),
            "comments",
            "comments",
            "body"
        ),
        vec!["second".to_owned()]
    );

    db.delete("todos", row(0x21)).unwrap();
    let after_parent_delete = snapshot_from_event(block_on(subscription.next_event()).unwrap());
    assert_eq!(after_parent_delete.root_count, 0);
    assert!(
        after_parent_delete.edges.is_empty(),
        "parent removal must cascade assembled child entries away"
    );
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
    let query = Query::from("todos")
        .array_subquery(ArraySubquery::new("comments", "comments", "todo_id", "id"));
    let prepared_query = prepared(&db, &query);
    let mut subscription = block_on(db.subscribe(&prepared_query, ReadOpts::default())).unwrap();

    let mut snapshot = snapshot_from_event(block_on(subscription.next_event()).unwrap());
    assert_eq!(
        ordered_limited_related_text_values(
            &snapshot,
            &schema,
            "todos",
            row(0x31),
            "comments",
            "comments",
            "body",
            1
        ),
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
    apply_subscription_event(&mut snapshot, block_on(subscription.next_event()).unwrap());
    assert_eq!(
        ordered_limited_related_text_values(
            &snapshot,
            &schema,
            "todos",
            row(0x31),
            "comments",
            "comments",
            "body",
            1
        ),
        vec!["b".to_owned()]
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
    if let Some(outside_boundary_event) = subscription.try_next_event() {
        apply_subscription_event(&mut snapshot, outside_boundary_event);
        assert_eq!(
            ordered_limited_related_text_values(
                &snapshot,
                &schema,
                "todos",
                row(0x31),
                "comments",
                "comments",
                "body",
                1
            ),
            vec!["b".to_owned()]
        );
    }

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
    apply_subscription_event(&mut snapshot, block_on(subscription.next_event()).unwrap());
    assert_eq!(
        ordered_limited_related_text_values(
            &snapshot,
            &schema,
            "todos",
            row(0x31),
            "comments",
            "comments",
            "body",
            1
        ),
        vec!["a".to_owned()]
    );

    db.update(
        "comments",
        row(0xd3),
        BTreeMap::from([("body".to_owned(), Value::String("z".to_owned()))]),
    )
    .unwrap();
    db.tick().unwrap();
    apply_subscription_event(&mut snapshot, block_on(subscription.next_event()).unwrap());
    assert_eq!(
        ordered_limited_related_text_values(
            &snapshot,
            &schema,
            "todos",
            row(0x31),
            "comments",
            "comments",
            "body",
            1
        ),
        vec!["b".to_owned()]
    );
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
        sorted_related_text_values(
            &admin,
            &schema,
            "todos",
            row(0x41),
            "comments",
            "comments",
            "body"
        ),
        vec!["member-visible".to_owned(), "other-visible".to_owned()]
    );

    let member_snapshot = block_on(db.all_relation_snapshot_for_identity(
        &prepared_query,
        ReadOpts::default(),
        member,
    ))
    .unwrap();
    assert_eq!(
        sorted_related_text_values(
            &member_snapshot,
            &schema,
            "todos",
            row(0x41),
            "comments",
            "comments",
            "body"
        ),
        vec!["member-visible".to_owned()]
    );

    let spy_snapshot =
        block_on(db.all_relation_snapshot_for_identity(&prepared_query, ReadOpts::default(), spy))
            .unwrap();
    assert_eq!(
        sorted_related_text_values(
            &spy_snapshot,
            &schema,
            "todos",
            row(0x41),
            "comments",
            "comments",
            "body"
        ),
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
        sorted_related_text_values(
            &maintained,
            &schema,
            "todos",
            row(0x51),
            "comments",
            "comments",
            "body"
        ),
        sorted_related_text_values(
            &one_shot,
            &schema,
            "todos",
            row(0x51),
            "comments",
            "comments",
            "body"
        )
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
    let snapshot = snapshot_from_event(block_on(subscription.next_event()).unwrap());
    assert_eq!(snapshot.root_count, 1);
    let root = snapshot
        .rows
        .iter()
        .find(|candidate| candidate.table() == "todos" && candidate.row_uuid() == row(0x52))
        .expect("late root should be present");
    assert_eq!(
        root.cell(schema_table(&schema, "todos"), "title"),
        Some(Value::String("late root".to_owned()))
    );
    assert_eq!(root.cell(schema_table(&schema, "todos"), "owner_id"), None);
    assert_eq!(
        sorted_related_text_values(
            &snapshot,
            &schema,
            "todos",
            row(0x52),
            "owner",
            "users",
            "name"
        ),
        vec!["owner".to_owned()]
    );
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
    let snapshot = snapshot_from_event(block_on(subscription.next_event()).unwrap());
    assert_eq!(snapshot.root_count, 1);
    assert_eq!(
        sorted_related_text_values(
            &snapshot,
            &schema,
            "issues",
            row(0x53),
            "project",
            "projects",
            "name"
        ),
        vec!["project".to_owned()]
    );
}

#[test]
fn array_subquery_remote_subscription_hydrates_edge_referenced_child_rows() {
    let schema = relation_schema();
    let server = open_core(0x5e, AuthorId::SYSTEM, &schema);
    let client_author = AuthorId::from_bytes([0xc6; 16]);
    let client = open_db(0xc6, client_author, &schema);
    let (client_transport, server_transport) = duplex();
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
            if sorted_related_text_values(
                &snapshot,
                &schema,
                "users",
                row(0xa6),
                "todosViaOwner",
                "todos",
                "title",
            ) == vec!["remote child".to_owned()]
            {
                delivered = Some(snapshot);
                break;
            }
        }
    }
    assert!(
        delivered.is_some(),
        "remote maintained array subscription must hydrate the child row referenced by relation-edge facts"
    );
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
    let mut tx = db.mergeable_tx();

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
    let mut tx = db.mergeable_tx();

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

    let mut tx = db.mergeable_tx();
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
    let mut tx = db.mergeable_tx();

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
    let mut tx = db.mergeable_tx();
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
        pending_upstream_subscribe_count(&db),
        1,
        "second propagating registrant should share upstream coverage"
    );

    drop(first);
    assert_eq!(
        pending_upstream_unsubscribe_count(&db),
        0,
        "upstream coverage stays live while another propagating registrant remains"
    );

    drop(second);
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

    let mut tx = db.mergeable_tx();
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
                | FEATURE_STRUCTURED_ERRORS,
            Some(session.clone()),
        )),
        Box::new(WireTransportAdapter::new(
            right,
            WIRE_PROTOCOL_VERSION,
            FEATURE_SYNC_MESSAGE_PAYLOAD
                | crate::wire::FEATURE_SESSION_FRAME
                | FEATURE_STRUCTURED_ERRORS,
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

fn joined_issue_query() -> Query {
    Query::from("issues").join_via("issue_tags", "issue", [eq(col("tag"), lit("prepared"))])
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
        let tx_id = self.server.node().borrow_mut().open_exclusive()?;
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
            .apply_sync_message(SyncMessage::PublishSchema {
                author: self.author,
                schema: Box::new(schema),
            })
            .map_err(Into::into)
    }

    fn publish_lens(&self, lens: MigrationLens) -> Result<Vec<SyncMessage>, Error> {
        self.server
            .node()
            .borrow_mut()
            .apply_sync_message(SyncMessage::PublishLens {
                author: self.author,
                lens,
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
            .apply_sync_message(SyncMessage::SetCurrentWriteSchema {
                author: self.author,
                pointer,
            })
            .map_err(Into::into)
    }
}

struct CoreExclusiveTx<'a> {
    core: &'a CoreDb,
    tx_id: OpenTxId,
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

    let schema_ack = core.publish_schema(schema_version.clone()).unwrap();
    assert!(matches!(
        schema_ack.as_slice(),
        [SyncMessage::CatalogueAck(ack)] if ack.schema == Some(schema_version.id) && ack.applied
    ));

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
    let lens_ack = core.publish_lens(lens.clone()).unwrap();
    assert!(matches!(
        lens_ack.as_slice(),
        [SyncMessage::CatalogueAck(ack)] if ack.lens == Some(lens.id) && ack.applied
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
fn oversized_view_update_splits_into_bounded_final_settling_chunks() {
    let subscription = SubscriptionKey {
        shape_id: ShapeId(uuid::Uuid::from_bytes([0x22; 16])),
        binding_id: BindingId(uuid::Uuid::from_bytes([0x33; 16])),
        read_view: RegisterShapeOptions::default().read_view_key(),
    };
    let facts = (0..700)
        .map(|idx| {
            crate::protocol::ProgramFactEntry::SourceCoverage(
                crate::protocol::SourceCoverageEntry {
                    source: format!("source-{idx}"),
                    table: "todos".to_owned().into(),
                    row: None,
                    coverage: vec![idx as u8; 4096],
                },
            )
        })
        .collect::<Vec<_>>();
    let update = SyncMessage::ViewUpdate {
        subscription,
        settled_through: GlobalSeq(42),
        reset_result_set: true,
        version_carriers: Vec::new(),
        version_bundles: Vec::new(),
        peer_payload_inventory: Default::default(),
        result_member_adds: Vec::new(),
        result_member_removes: Vec::new(),
        program_fact_adds: facts,
        program_fact_removes: Vec::new(),
    };
    assert!(serialized_sync_message_len(&update) > MAX_SYNC_MESSAGE_BYTES);

    let chunks = split_oversized_view_update(update).unwrap();
    assert!(chunks.len() > 1);
    for (idx, chunk) in chunks.iter().enumerate() {
        assert!(serialized_sync_message_len(chunk) <= MAX_SYNC_MESSAGE_BYTES);
        assert!(serialized_uncompressed_wire_message_len(chunk) <= MAX_WIRE_FRAME_BYTES);
        let SyncMessage::ViewUpdateChunk {
            reset_result_set,
            final_chunk,
            ..
        } = chunk
        else {
            panic!("expected chunked view update");
        };
        assert_eq!(*reset_result_set, idx == 0);
        assert_eq!(*final_chunk, idx + 1 == chunks.len());
    }
}

#[test]
fn view_update_chunking_budgets_full_wire_frame_boundary() {
    let subscription = SubscriptionKey {
        shape_id: ShapeId(uuid::Uuid::from_bytes([0x24; 16])),
        binding_id: BindingId(uuid::Uuid::from_bytes([0x35; 16])),
        read_view: RegisterShapeOptions::default().read_view_key(),
    };

    let mut low = 0usize;
    let mut high = 800usize;
    let mut prefix_count = 0usize;
    while low <= high {
        let mid = low + (high - low) / 2;
        let candidate = view_update_with_facts(subscription, source_coverage_facts(mid, 4096));
        if serialized_sync_message_len(&candidate) < MAX_SYNC_MESSAGE_BYTES - 20_000 {
            prefix_count = mid;
            low = mid + 1;
        } else {
            high = mid.saturating_sub(1);
        }
    }
    let prefix = source_coverage_facts(prefix_count, 4096);

    let mut low = 0usize;
    let mut high = 50_000usize;
    let mut tail_len = None;
    while low <= high {
        let mid = low + (high - low) / 2;
        let mut candidate_facts = prefix.clone();
        candidate_facts.push(source_coverage_fact(candidate_facts.len(), mid));
        let candidate = view_update_with_facts(subscription, candidate_facts);
        if serialized_sync_message_len(&candidate) <= MAX_SYNC_MESSAGE_BYTES {
            tail_len = Some(mid);
            low = mid + 1;
        } else {
            high = mid.saturating_sub(1);
        }
    }
    let mut facts = prefix;
    facts.push(source_coverage_fact(
        facts.len(),
        tail_len.expect("test fixture should find a semantic-fit tail"),
    ));
    let update = view_update_with_facts(subscription, facts);
    assert!(serialized_sync_message_len(&update) <= MAX_SYNC_MESSAGE_BYTES);
    assert!(serialized_uncompressed_wire_message_len(&update) > MAX_WIRE_FRAME_BYTES);

    let chunks = split_oversized_view_update(update).unwrap();
    assert!(chunks.len() > 1);
    for chunk in &chunks {
        assert!(
            serialized_uncompressed_wire_message_len(chunk) <= MAX_WIRE_FRAME_BYTES,
            "chunk framed length {} exceeds cap {}",
            serialized_uncompressed_wire_message_len(chunk),
            MAX_WIRE_FRAME_BYTES
        );
    }
}

#[test]
fn view_update_chunking_keeps_result_adds_with_referenced_versions() {
    // Internal protocol test: the public API only exposes eventual subscription
    // rows, while this pins the chunk composition invariant that prevents
    // per-chunk missing-ref repair from seeing false misses.
    let schema = schema();
    let table = &schema.tables[0];
    let subscription = SubscriptionKey {
        shape_id: ShapeId(uuid::Uuid::from_bytes([0x25; 16])),
        binding_id: BindingId(uuid::Uuid::from_bytes([0x36; 16])),
        read_view: RegisterShapeOptions::default().read_view_key(),
    };
    let tx_node = NodeUuid::from_bytes([0x51; 16]);
    let mut version_bundles = Vec::new();
    let mut result_member_adds = Vec::new();
    for idx in 0..900u16 {
        let tx_id = TxId::new(TxTime::from(idx as u64 + 1), tx_node);
        let row_uuid = RowUuid::from_bytes([
            (idx >> 8) as u8,
            idx as u8,
            0xaa,
            0xaa,
            0xaa,
            0xaa,
            0xaa,
            0xaa,
            0xaa,
            0xaa,
            0xaa,
            0xaa,
            0xaa,
            0xaa,
            0xaa,
            0xaa,
        ]);
        let tx = crate::tx::Transaction {
            tx_id,
            kind: crate::tx::TxKind::Mergeable,
            n_total_writes: 1,
            made_by: AuthorId::SYSTEM,
            permission_subject: None,
            base_snapshot: None,
            row_read_set: None,
            absent_read_set: None,
            predicate_read_set: None,
            user_metadata_json: None,
            source_branch: None,
            merge_strategy: None,
        };
        let version = crate::protocol::VersionRecord::from_cells(
            table,
            schema.version_id(),
            row_uuid,
            Vec::new(),
            AuthorId::SYSTEM,
            TxTime(1),
            AuthorId::SYSTEM,
            TxTime(1),
            &BTreeMap::from([
                (
                    "title".to_owned(),
                    Value::String(format!("row-{idx}-{}", "x".repeat(4096))),
                ),
                ("done".to_owned(), Value::Bool(false)),
                ("owner".to_owned(), Value::Uuid(AuthorId::SYSTEM.0)),
            ]),
            None,
        )
        .unwrap();
        version_bundles.push(VersionBundle {
            tx,
            versions: vec![version],
            fate: Fate::Accepted,
            global_seq: Some(GlobalSeq(idx as u64 + 1)),
            durability: DurabilityTier::Global,
        });
        result_member_adds.push(ResultMemberEntry::row((
            "todos".to_owned().into(),
            row_uuid,
            tx_id,
        )));
    }

    let update = SyncMessage::ViewUpdate {
        subscription,
        settled_through: GlobalSeq(900),
        reset_result_set: true,
        version_carriers: Vec::new(),
        version_bundles,
        peer_payload_inventory: Default::default(),
        result_member_adds,
        result_member_removes: Vec::new(),
        program_fact_adds: Vec::new(),
        program_fact_removes: Vec::new(),
    };
    assert!(serialized_sync_message_len(&update) > MAX_SYNC_MESSAGE_BYTES);

    let chunks = split_oversized_view_update(update).unwrap();
    assert!(chunks.len() > 1);
    let mut total_adds = 0;
    let mut saw_version_carrier = false;
    let mut saw_run_carrier = false;
    for chunk in chunks {
        assert!(serialized_uncompressed_wire_message_len(&chunk) <= MAX_WIRE_FRAME_BYTES);
        let SyncMessage::ViewUpdateChunk {
            version_carriers,
            version_bundles,
            result_member_adds,
            ..
        } = chunk
        else {
            panic!("expected chunked view update");
        };
        assert!(
            version_bundles.is_empty(),
            "chunked version payloads should be emitted as carriers"
        );
        saw_version_carrier |= !version_carriers.is_empty();
        saw_run_carrier |= version_carriers
            .iter()
            .any(|carrier| matches!(carrier, crate::protocol::VersionCarrier::Run(_)));
        let expanded_bundles = crate::protocol::expand_version_carriers(&version_carriers)
            .expect("chunk carriers should expand");
        let incoming = expanded_bundles
            .iter()
            .flat_map(|bundle| {
                bundle.versions.iter().map(|version| {
                    RowVersionRef::new(
                        version.table().to_owned(),
                        version.row_uuid(),
                        bundle.tx.tx_id,
                    )
                })
            })
            .collect::<BTreeSet<_>>();
        for (table, row_uuid, tx_id) in result_member_adds
            .iter()
            .filter_map(ResultMemberEntry::as_row)
        {
            total_adds += 1;
            assert!(
                incoming.contains(&RowVersionRef::new(table.to_string(), row_uuid, tx_id)),
                "result add must be accompanied by its referenced version in the same chunk"
            );
        }
    }
    assert!(saw_version_carrier);
    assert!(saw_run_carrier);
    assert_eq!(total_adds, 900);
}

#[test]
fn oversized_snapshot_subscription_delivers_full_settled_count() {
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

    let (client_transport, server_transport) = duplex();
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
        "oversized snapshot subscription did not settle; currently visible rows={}",
        rows.len()
    );
}

fn view_update_with_facts(
    subscription: SubscriptionKey,
    facts: Vec<crate::protocol::ProgramFactEntry>,
) -> SyncMessage {
    SyncMessage::ViewUpdate {
        subscription,
        settled_through: GlobalSeq(42),
        reset_result_set: true,
        version_carriers: Vec::new(),
        version_bundles: Vec::new(),
        peer_payload_inventory: Default::default(),
        result_member_adds: Vec::new(),
        result_member_removes: Vec::new(),
        program_fact_adds: facts,
        program_fact_removes: Vec::new(),
    }
}

fn source_coverage_fact(idx: usize, coverage_len: usize) -> crate::protocol::ProgramFactEntry {
    crate::protocol::ProgramFactEntry::SourceCoverage(crate::protocol::SourceCoverageEntry {
        source: format!("boundary-source-{idx}"),
        table: "todos".to_owned().into(),
        row: None,
        coverage: vec![idx as u8; coverage_len],
    })
}

fn source_coverage_facts(
    count: usize,
    coverage_len: usize,
) -> Vec<crate::protocol::ProgramFactEntry> {
    (0..count)
        .map(|idx| source_coverage_fact(idx, coverage_len))
        .collect()
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
        client_transport
            .try_recv()
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
        client_transport
            .try_recv()
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
        client_transport
            .try_recv()
            .expect("expected branch subscription rejection"),
        branch_subscription,
    );
    subscriber.borrow_mut().tick().unwrap();

    seed(&server, "todos", cells("second", false, owner));
    subscriber.borrow_mut().tick().unwrap();
    assert_view_update_for_subscription(
        client_transport
            .try_recv()
            .expect("expected supported update after rejection"),
        supported_subscription,
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

    client_transport
        .send(SyncMessage::RegisterShape {
            shape_id: shape.shape_id(),
            ast: ShapeAst::from_validated(&shape),
            opts,
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
        client_transport
            .try_recv()
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
        client_transport.try_recv().is_none(),
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
        client_transport
            .try_recv()
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
        client_transport
            .try_recv()
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
        client_transport
            .try_recv()
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

    client_transport
        .send(SyncMessage::RegisterShape {
            shape_id: shape.shape_id(),
            ast: ShapeAst::from_validated(&shape),
            opts: edge_opts,
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
        client_transport.try_recv().is_none(),
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
    }) = client_transport.try_recv()
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
        .apply_sync_message(SyncMessage::SetCurrentWriteSchema {
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

#[test]
fn repeated_identical_session_claims_emit_once_before_subscribe() {
    let schema = schema();
    let client_author = AuthorId::from_bytes([0xc1; 16]);
    let client = open_db(0xc1, client_author, &schema);
    let (client_transport, mut upstream_transport) = duplex();
    let claims = BTreeMap::from([("role".to_owned(), Value::String("reader".to_owned()))]);

    client.set_identity_claims(client_author, claims.clone());
    client.set_identity_claims(client_author, claims);
    let _upstream = client.connect_upstream(client_transport);

    let query = Query::from("todos").filter(eq(col("done"), lit(false)));
    let _subscription = prepared_subscribe(&client, &query, global_subscribe_opts()).unwrap();

    client.tick().unwrap();
    assert!(matches!(
        upstream_transport.try_recv(),
        Some(SyncMessage::SessionClaims { .. })
    ));
    assert!(matches!(
        upstream_transport.try_recv(),
        Some(SyncMessage::RegisterShape { .. })
    ));
    assert!(matches!(
        upstream_transport.try_recv(),
        Some(SyncMessage::Subscribe(_))
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
    let second_tx = second.commit().unwrap();

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
    assert_eq!(
        client.write_state(second_tx).unwrap(),
        WriteState {
            fate: Fate::Rejected(RejectionReason::ExclusiveConflict),
            durability: DurabilityTier::Local,
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

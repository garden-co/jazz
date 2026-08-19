//! Shared test transports, fixtures, and assertion helpers.

use super::*;

/// Receive the next subscriber payload relevant to direct protocol assertions.
/// A subscriber begins by publishing its trusted catalogue prerequisite; tests
/// that do not model a receiving `Db` still need to consume that control-plane
/// message before asserting the requested registration/subscription response.
pub(super) fn try_recv_subscriber_payload(transport: &mut dyn Transport) -> Option<SyncMessage> {
    loop {
        match transport.try_recv()? {
            SyncMessage::CatalogueSnapshot(_) => continue,
            message => return Some(message),
        }
    }
}

pub(super) struct BackpressureOnceTransport {
    pub(super) outbound: Rc<RefCell<std::collections::VecDeque<SyncMessage>>>,
    pub(super) failed: bool,
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
pub(super) struct ByteDuplexTransport {
    pub(super) outbound: Rc<RefCell<std::collections::VecDeque<Vec<u8>>>>,
    pub(super) inbound: Rc<RefCell<std::collections::VecDeque<Vec<u8>>>>,
}

pub(super) struct OneShotBackpressureTransport {
    pub(super) outbound: Rc<RefCell<std::collections::VecDeque<Vec<u8>>>>,
    pub(super) calls: usize,
    pub(super) fail_on_call: usize,
    pub(super) failed: bool,
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

pub(super) fn byte_duplex_raw() -> (ByteDuplexTransport, ByteDuplexTransport) {
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

pub(super) fn byte_duplex() -> (Box<dyn Transport>, Box<dyn Transport>) {
    let (left, right) = byte_duplex_raw();
    (
        Box::new(WireTransportAdapter::current(left)),
        Box::new(WireTransportAdapter::current(right)),
    )
}

pub(super) fn byte_duplex_uncompressed() -> (Box<dyn Transport>, Box<dyn Transport>) {
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

pub(super) fn rocks_storage(schema: &JazzSchema) -> RocksDbStorage {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.keep();
    let cfs = schema.column_families();
    let refs = cfs.iter().map(String::as_str).collect::<Vec<_>>();
    RocksDbStorage::open(&path, &refs).unwrap()
}

pub(super) fn open_db(node: u8, author: AuthorId, schema: &JazzSchema) -> Db<RocksDbStorage> {
    let storage = rocks_storage(schema);
    block_on(Db::open(DbConfig {
        schema: schema.clone(),
        storage,
        identity: DbIdentity {
            node: NodeUuid::from_bytes([node; 16]),
            author,
        },
        id_source: Some(Box::new(SeededRowIdSource::new(node as u64))),
    }))
    .unwrap()
}

pub(super) fn row_ids(rows: &[CurrentRow]) -> Vec<RowUuid> {
    rows.iter().map(CurrentRow::row_uuid).collect()
}

/// Client writes stage locally before the authority evaluates their policy.
/// Keep this assertion at the sync boundary so tests do not accidentally
/// reintroduce synchronous local policy enforcement for ordinary writes.

/// In-memory transport pair: each side's outbound queue is the other's
/// inbound queue, so a `send` lands directly in the peer's `try_recv`.
pub(super) struct DuplexTransport {
    outbound: Rc<RefCell<std::collections::VecDeque<SyncMessage>>>,
    inbound: Rc<RefCell<std::collections::VecDeque<SyncMessage>>>,
    session_context: Option<ConnectionSessionContext>,
}

impl Transport for DuplexTransport {
    fn send(&mut self, message: SyncMessage) -> Result<(), TransportError> {
        self.outbound.borrow_mut().push_back(message);
        Ok(())
    }

    fn try_recv(&mut self) -> Option<SyncMessage> {
        self.inbound.borrow_mut().pop_front()
    }

    fn connection_session_context(&self) -> Option<ConnectionSessionContext> {
        self.session_context
    }
}

pub(super) fn duplex() -> (Box<dyn Transport>, Box<dyn Transport>) {
    use std::collections::VecDeque;
    let left = Rc::new(RefCell::new(VecDeque::new()));
    let right = Rc::new(RefCell::new(VecDeque::new()));
    (
        Box::new(DuplexTransport {
            outbound: Rc::clone(&left),
            inbound: Rc::clone(&right),
            session_context: None,
        }),
        Box::new(DuplexTransport {
            outbound: right,
            inbound: left,
            session_context: None,
        }),
    )
}

/// In-memory transport pair with a read-only tap on server-to-client frames.
/// The tap lets a Core-serving test inspect the canonical `ViewUpdate` before
/// the receiving edge applies it.
pub(super) fn duplex_with_server_outbound_tap() -> (
    Box<dyn Transport>,
    Box<dyn Transport>,
    Rc<RefCell<std::collections::VecDeque<SyncMessage>>>,
) {
    use std::collections::VecDeque;
    let client_to_server = Rc::new(RefCell::new(VecDeque::new()));
    let server_to_client = Rc::new(RefCell::new(VecDeque::new()));
    (
        Box::new(DuplexTransport {
            outbound: Rc::clone(&client_to_server),
            inbound: Rc::clone(&server_to_client),
            session_context: None,
        }),
        Box::new(DuplexTransport {
            outbound: Rc::clone(&server_to_client),
            inbound: client_to_server,
            session_context: None,
        }),
        server_to_client,
    )
}

/// In-memory transport pair with a read-only tap on client-to-server frames.
/// The tap lets an Edge test inspect an upstream upload before Core applies it.
pub(super) fn duplex_with_client_outbound_tap() -> (
    Box<dyn Transport>,
    Box<dyn Transport>,
    Rc<RefCell<std::collections::VecDeque<SyncMessage>>>,
) {
    use std::collections::VecDeque;
    let client_to_server = Rc::new(RefCell::new(VecDeque::new()));
    let server_to_client = Rc::new(RefCell::new(VecDeque::new()));
    (
        Box::new(DuplexTransport {
            outbound: Rc::clone(&client_to_server),
            inbound: Rc::clone(&server_to_client),
            session_context: None,
        }),
        Box::new(DuplexTransport {
            outbound: server_to_client,
            inbound: Rc::clone(&client_to_server),
            session_context: None,
        }),
        client_to_server,
    )
}

/// In-memory handshake pairing needs an internal test because it verifies the
/// transport/admission boundary before any user-visible sync payload exists.
pub(super) fn duplex_with_admitted_session_context(
    identity: AuthorId,
    client_node: NodeUuid,
    client_epoch: u64,
    server_node: NodeUuid,
    server_epoch: u64,
) -> (Box<dyn Transport>, Box<dyn Transport>) {
    use std::collections::VecDeque;
    let left = Rc::new(RefCell::new(VecDeque::new()));
    let right = Rc::new(RefCell::new(VecDeque::new()));
    let features = crate::wire::FEATURE_AUTHORIZATION_SCOPE_VIEWS;
    let client = ConnectionSessionContext {
        local: crate::wire::WireAuthorityEndpoint {
            node: client_node,
            epoch: client_epoch,
        },
        remote: crate::wire::WireAuthorityEndpoint {
            node: server_node,
            epoch: server_epoch,
        },
        link_identity: identity,
        negotiated_features: features,
    };
    let server = ConnectionSessionContext {
        local: client.remote,
        remote: client.local,
        link_identity: identity,
        negotiated_features: features,
    };
    (
        Box::new(DuplexTransport {
            outbound: Rc::clone(&left),
            inbound: Rc::clone(&right),
            session_context: Some(client),
        }),
        Box::new(DuplexTransport {
            outbound: right,
            inbound: left,
            session_context: Some(server),
        }),
    )
}

pub(super) fn block_on<F: Future>(future: F) -> F::Output {
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

pub(super) fn apply_subscription_event(snapshot: &mut RelationSnapshot, event: SubscriptionEvent) {
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

pub(super) fn opened_rows(event: SubscriptionEvent) -> Vec<CurrentRow> {
    let mut snapshot = RelationSnapshot::default();
    apply_subscription_event(&mut snapshot, event);
    snapshot.rows
}

pub(super) fn pending_upstream_subscribe_count<S>(db: &Db<S>) -> usize
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

pub(super) fn pending_upstream_unsubscribe_count<S>(db: &Db<S>) -> usize
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

pub(super) fn decode_wire_message_payload(
    decoder: &mut WireStreamDecoder,
    envelope: &crate::wire::WireEnvelope,
) -> SyncMessage {
    let payload = decoder
        .decode_message(&envelope.payload, envelope.features)
        .unwrap();
    decode_sync_message(&payload).unwrap()
}

pub(super) fn delta_rows(
    event: SubscriptionEvent,
) -> (Vec<CurrentRow>, Vec<CurrentRow>, Vec<RemovedRow>) {
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

pub(super) fn snapshot_from_event(event: SubscriptionEvent) -> RelationSnapshot {
    let mut snapshot = RelationSnapshot::default();
    apply_subscription_event(&mut snapshot, event);
    snapshot
}

/// A maintained union emits a suffix removal addressed to its typed union arm.
/// Alice's one source row appears through `left` and `right` union arms; only
/// the left occurrence leaves the ordered result.
pub(super) fn terminal_nested_text_values(
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

pub(super) fn terminal_nested_values(
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

pub(super) fn oversized_row_version_refs(len: usize) -> Vec<RowVersionRef> {
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

pub(super) fn event_settled(event: &SubscriptionEvent) -> bool {
    match event {
        SubscriptionEvent::Delta { settled, .. } => *settled,
        SubscriptionEvent::Rejected { .. } => false,
        SubscriptionEvent::Closed => false,
    }
}

pub(super) fn global_subscribe_opts() -> ReadOpts {
    ReadOpts {
        tier: DurabilityTier::Global,
        local_updates: LocalUpdates::Deferred,
        propagation: Propagation::Full,
        include_deleted: false,
        ..ReadOpts::default()
    }
}

pub(super) fn edge_subscribe_opts() -> ReadOpts {
    ReadOpts {
        tier: DurabilityTier::Edge,
        local_updates: LocalUpdates::Deferred,
        propagation: Propagation::Full,
        include_deleted: false,
        ..ReadOpts::default()
    }
}

pub(super) fn branch_read_opts() -> ReadOpts {
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

pub(super) fn assert_unsupported_subscription_include_deleted(error: Error) {
    assert_eq!(error.code, ErrorCode::Query);
    assert!(
        error.message.contains("include_deleted"),
        "unexpected error message: {}",
        error.message
    );
}

pub(super) fn assert_subscribe_rejected_unsupported_shape_capability_detail(
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

pub(super) fn assert_view_update_for_subscription(
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

pub(super) fn expect_error<T>(result: Result<T, Error>) -> Error {
    match result {
        Ok(_) => panic!("expected operation to fail"),
        Err(error) => error,
    }
}

pub(super) fn prepared<S>(db: &Db<S>, query: &Query) -> PreparedQuery
where
    S: OrderedKvStorage + ReopenableStorage + 'static,
{
    db.prepare_query(query).unwrap()
}

pub(super) fn prepared_read<S>(db: &Db<S>, query: &Query) -> Vec<CurrentRow>
where
    S: OrderedKvStorage + ReopenableStorage + 'static,
{
    let prepared = prepared(db, query);
    db.read(&prepared).unwrap()
}

pub(super) fn prepared_one<S>(db: &Db<S>, query: &Query) -> Option<CurrentRow>
where
    S: OrderedKvStorage + ReopenableStorage + 'static,
{
    let prepared = prepared(db, query);
    db.one(&prepared).unwrap()
}

pub(super) fn prepared_all<S>(db: &Db<S>, query: &Query, opts: ReadOpts) -> Vec<CurrentRow>
where
    S: OrderedKvStorage + ReopenableStorage + 'static,
{
    let prepared = prepared(db, query);
    block_on(db.all(&prepared, opts)).unwrap()
}

pub(super) fn prepared_subscribe<S>(
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
pub(super) struct RecordingScheduler {
    calls: RefCell<Vec<TickUrgency>>,
}

impl TickScheduler for RecordingScheduler {
    fn schedule_tick(&self, urgency: TickUrgency) {
        self.calls.borrow_mut().push(urgency);
    }
}

impl RecordingScheduler {
    pub(super) fn take(&self) -> Vec<TickUrgency> {
        std::mem::take(&mut self.calls.borrow_mut())
    }
}

pub(super) fn schema() -> JazzSchema {
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

pub(super) fn payload_enum_query_schema() -> JazzSchema {
    let event = ColumnType::Enum(Box::new(
        EnumSchema::new(
            "event",
            [
                EnumCase::new(
                    "message",
                    RecordDescriptor::new([("level", ValueType::I32)]),
                ),
                EnumCase::new("closed", RecordDescriptor::new([("code", ValueType::I32)])),
            ],
        )
        .unwrap(),
    ));
    JazzSchema::new([
        TableSchema::new("events", [ColumnSchema::new("event", event)])
            .with_read_policy(Policy::public())
            .with_write_policy(Policy::public()),
    ])
}

pub(super) fn payload_message(level: i32) -> Value {
    Value::Enum(
        EnumValue::create(
            0,
            RecordDescriptor::new([("level", ValueType::I32)]),
            &[Value::I32(level)],
        )
        .unwrap(),
    )
}

pub(super) fn payload_closed(code: i32) -> Value {
    Value::Enum(
        EnumValue::create(
            1,
            RecordDescriptor::new([("code", ValueType::I32)]),
            &[Value::I32(code)],
        )
        .unwrap(),
    )
}

pub(super) fn owner_read_schema() -> JazzSchema {
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

pub(super) fn created_by_read_schema() -> JazzSchema {
    created_by_read_schema_for_claim("sub")
}

pub(super) fn created_by_read_schema_for_claim(claim_name: &str) -> JazzSchema {
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

pub(super) fn owner_write_schema() -> JazzSchema {
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

pub(super) fn editor_claim_write_schema() -> JazzSchema {
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

pub(super) fn owner_id_read_schema() -> JazzSchema {
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

pub(super) fn owner_id_public_schema() -> JazzSchema {
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

pub(super) fn owner_id_session_write_schema() -> JazzSchema {
    JazzSchema::new([TableSchema::new(
        "messages",
        [
            ColumnSchema::new("body", ColumnType::String),
            ColumnSchema::new("owner_id", ColumnType::String),
        ],
    )
    .with_read_policy(Policy::public())
    .with_write_policy(Policy::shape(
        Query::from("messages").filter(eq(col("owner_id"), claim("user_id"))),
    ))])
}

pub(super) fn owner_uuid_session_write_schema() -> JazzSchema {
    JazzSchema::new([TableSchema::new(
        "messages",
        [
            ColumnSchema::new("body", ColumnType::String),
            ColumnSchema::new("owner_id", ColumnType::Uuid),
        ],
    )
    .with_read_policy(Policy::public())
    .with_write_policy(Policy::shape(
        Query::from("messages").filter(eq(col("owner_id"), claim("user_id"))),
    ))])
}

pub(super) fn benchmark_shaped_recursive_reachable_read_schema() -> JazzSchema {
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

pub(super) fn customer_resource_policy_minimal_schema() -> JazzSchema {
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

pub(super) fn customer_two_resource_policy_minimal_schema() -> JazzSchema {
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

pub(super) fn same_table_seeded_resource_policy_schema() -> JazzSchema {
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

pub(super) fn same_table_string_seeded_resource_policy_schema() -> JazzSchema {
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

pub(super) fn customer_inherited_child_policy_schema() -> JazzSchema {
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

pub(super) fn inherited_insert_policy_schema() -> JazzSchema {
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

pub(super) fn resource_columns_for_customer_fixture() -> [ColumnSchema; 13] {
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

pub(super) fn relation_schema() -> JazzSchema {
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

pub(super) fn membership_scoped_relation_schema() -> JazzSchema {
    JazzSchema::new([
        TableSchema::new(
            "chats",
            [
                ColumnSchema::new("name", ColumnType::String),
                ColumnSchema::new("is_public", ColumnType::Bool),
                ColumnSchema::new("created_by", ColumnType::String),
                ColumnSchema::new("join_code", ColumnType::String.nullable()),
            ],
        )
        .with_read_policy(Policy::shape(
            Query::from("chats")
                .filter(any_of([]))
                .policy_branch(PolicyBranch::single_alternative_from_query(
                    Query::from("chats").filter(eq(col("is_public"), lit(true))),
                ))
                .policy_branch(PolicyBranch::single_alternative_from_query(
                    Query::from("chats").filter(eq(col("join_code"), claim("join_code"))),
                ))
                .policy_branch(PolicyBranch::single_alternative_from_query(
                    Query::from("chats").join_via_column(
                        "chat_members",
                        "chat_id",
                        "id",
                        [eq(col("user_id"), claim("user_id"))],
                    ),
                )),
        ))
        .with_write_policy(Policy::public()),
        TableSchema::new(
            "chat_members",
            [
                ColumnSchema::new("chat_id", ColumnType::Uuid),
                ColumnSchema::new("user_id", ColumnType::String),
                ColumnSchema::new("join_code", ColumnType::String.nullable()),
            ],
        )
        .with_reference("chat_id", "chats")
        .with_read_policy(Policy::shape(
            Query::from("chat_members")
                .filter(any_of([]))
                .policy_branch(PolicyBranch::single_alternative_from_query(
                    Query::from("chat_members").filter(eq(col("user_id"), claim("user_id"))),
                ))
                .policy_branch(PolicyBranch::single_alternative_from_query(
                    Query::from("chat_members").join_via_column(
                        "chat_members",
                        "chat_id",
                        "chat_id",
                        [eq(col("user_id"), claim("user_id"))],
                    ),
                )),
        ))
        .with_write_policy(Policy::public()),
        TableSchema::new(
            "profiles",
            [
                ColumnSchema::new("user_id", ColumnType::String),
                ColumnSchema::new("name", ColumnType::String),
                ColumnSchema::new("avatar", ColumnType::String.nullable()),
            ],
        )
        .with_read_policy(Policy::public())
        .with_write_policy(Policy::public()),
        TableSchema::new(
            "messages",
            [
                ColumnSchema::new("chat_id", ColumnType::Uuid),
                ColumnSchema::new("sender_id", ColumnType::Uuid),
                ColumnSchema::new("text", ColumnType::String),
                ColumnSchema::new("created_at", ColumnType::U64),
            ],
        )
        .with_reference("chat_id", "chats")
        .with_reference("sender_id", "profiles")
        .with_read_policy(Policy::shape(
            Query::from("messages")
                .filter(any_of([]))
                .policy_branch(PolicyBranch::single_alternative_from_query(
                    Query::from("messages").join_via_column(
                        "chats",
                        "id",
                        "chat_id",
                        [eq(col("is_public"), lit(true))],
                    ),
                ))
                .policy_branch(PolicyBranch::single_alternative_from_query(
                    Query::from("messages").join_via_column(
                        "chat_members",
                        "chat_id",
                        "chat_id",
                        [eq(col("user_id"), claim("user_id"))],
                    ),
                )),
        ))
        .with_write_policy(Policy::public()),
    ])
}

pub(super) fn relation_hop_schema() -> JazzSchema {
    JazzSchema::new([
        TableSchema::new("orgs", [ColumnSchema::new("name", ColumnType::String)])
            .with_read_policy(Policy::public())
            .with_write_policy(Policy::public()),
        TableSchema::new(
            "teams",
            [
                ColumnSchema::new("name", ColumnType::String),
                ColumnSchema::new("org_id", ColumnType::Nullable(Box::new(ColumnType::Uuid))),
                ColumnSchema::new(
                    "parent_id",
                    ColumnType::Nullable(Box::new(ColumnType::Uuid)),
                ),
            ],
        )
        .with_reference("org_id", "orgs")
        .with_reference("parent_id", "teams")
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

pub(super) fn access_edge_include_schema() -> JazzSchema {
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

pub(super) fn policy_relation_schema() -> JazzSchema {
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

pub(super) fn evolved_owner_write_schema() -> JazzSchema {
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

pub(super) fn row(byte: u8) -> RowUuid {
    RowUuid::from_bytes([byte; 16])
}

pub(super) fn relation_snapshot_row(table: &str, row_uuid: RowUuid) -> CurrentRow {
    let descriptor = RecordDescriptor::new([("row_uuid".to_owned(), ValueType::Uuid)]);
    let raw = descriptor
        .create(&[groove::records::Value::Uuid(row_uuid.0)])
        .expect("encode relation snapshot row");
    CurrentRow::new(table, OwnedRecord::new(raw, descriptor))
}

/// A reset may carry canonical relation provenance while the materialized
/// related row is named by a newer read schema. Ordinary removal must use the
/// same projected edge identity, retaining an unrelated same-UUID row.
pub(super) fn cells(title: &str, done: bool, owner: AuthorId) -> RowCells {
    BTreeMap::from([
        ("title".to_owned(), Value::String(title.to_owned())),
        ("done".to_owned(), Value::Bool(done)),
        ("owner".to_owned(), Value::Uuid(owner.0)),
    ])
}

pub(super) fn issue_schema() -> JazzSchema {
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

pub(super) fn issue_cells(
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

pub(super) struct CoreDb {
    pub(super) server: Node<RocksDbStorage>,
    schema: JazzSchema,
    author: AuthorId,
    pub(super) next_now_ms: Cell<u64>,
    id_source: RefCell<SeededRowIdSource>,
}

pub(super) fn open_core(node_byte: u8, author: AuthorId, schema: &JazzSchema) -> CoreDb {
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
    pub(super) fn node(&self) -> SharedNodeState<RocksDbStorage> {
        self.server.node()
    }

    pub(super) fn next_now_ms(&self) -> u64 {
        let next = self.next_now_ms.get();
        self.next_now_ms.set(next + 1);
        next
    }

    pub(super) fn table(&self, table: impl Into<String>) -> Query {
        Query::from(table)
    }

    pub(super) fn read(&self, query: &Query) -> Result<Vec<CurrentRow>, Error> {
        let shape = query.validate(&self.schema)?;
        let binding = shape.bind(BTreeMap::new())?;
        block_on(self.server.node().borrow_mut().query_rows(
            &shape,
            &binding,
            DurabilityTier::Local,
        ))
        .map_err(Into::into)
    }

    pub(super) fn one(&self, query: &Query) -> Result<Option<CurrentRow>, Error> {
        Ok(self.read(query)?.into_iter().next())
    }

    pub(super) fn at(&self, position: GlobalSeq, query: &Query) -> Result<Vec<CurrentRow>, Error> {
        let shape = query.validate(&self.schema)?;
        let binding = shape.bind(BTreeMap::new())?;
        block_on(
            self.server
                .node()
                .borrow_mut()
                .at(position)
                .read(&shape, &binding),
        )
        .map_err(Into::into)
    }

    pub(super) fn insert(
        &self,
        table: &str,
        cells: RowCells,
    ) -> Result<WriteHandle<RocksDbStorage>, Error> {
        let row = self.id_source.borrow_mut().next_row_id();
        self.insert_with_id(table, row, cells)
    }

    pub(super) fn insert_with_id(
        &self,
        table: &str,
        row: RowUuid,
        cells: RowCells,
    ) -> Result<WriteHandle<RocksDbStorage>, Error> {
        let node = self.server.node();
        let published = block_on(
            node.borrow_mut().commit_mergeable(
                MergeableCommit::new(table, row, self.next_now_ms())
                    .made_by(self.author)
                    .cells(cells),
            ),
        )?;
        let tx_id = block_on(node.borrow_mut().persist_and_settle_transaction(published))?;
        let outcome = block_on(node.borrow_mut().finalize_local_mergeable_commit(tx_id))?;
        block_on(node.borrow_mut().persist_and_settle_outcome(outcome))?;
        self.server.mark_subscriber_connections_dirty();
        Ok(WriteHandle {
            node: Rc::downgrade(&node),
            row_uuid: row,
            tx_id,
            local_tier: DurabilityTier::Global,
        })
    }

    pub(super) fn insert_attributed(
        &self,
        made_by: AuthorId,
        table: &str,
        cells: RowCells,
    ) -> Result<WriteHandle<RocksDbStorage>, Error> {
        let row = self.id_source.borrow_mut().next_row_id();
        let node = self.server.node();
        let published = block_on(
            node.borrow_mut().commit_mergeable(
                MergeableCommit::new(table, row, self.next_now_ms())
                    .made_by(made_by)
                    .permission_subject(self.author)
                    .cells(cells),
            ),
        )?;
        let tx_id = block_on(node.borrow_mut().persist_and_settle_transaction(published))?;
        let outcome = block_on(node.borrow_mut().finalize_local_mergeable_commit(tx_id))?;
        block_on(node.borrow_mut().persist_and_settle_outcome(outcome))?;
        self.server.mark_subscriber_connections_dirty();
        Ok(WriteHandle {
            node: Rc::downgrade(&node),
            row_uuid: row,
            tx_id,
            local_tier: DurabilityTier::Global,
        })
    }

    pub(super) fn update(
        &self,
        table: &str,
        row: RowUuid,
        patch: RowCells,
    ) -> Result<WriteHandle<RocksDbStorage>, Error> {
        self.update_attributed(self.author, table, row, patch)
    }

    pub(super) fn update_attributed(
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
            parent = block_on(self.server.node().borrow_mut().current_row_tx_id(&existing));
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
        let published = block_on(node.borrow_mut().commit_mergeable(commit))?;
        let tx_id = block_on(node.borrow_mut().persist_and_settle_transaction(published))?;
        let outcome = block_on(node.borrow_mut().finalize_local_mergeable_commit(tx_id))?;
        block_on(node.borrow_mut().persist_and_settle_outcome(outcome))?;
        self.server.mark_subscriber_connections_dirty();
        Ok(WriteHandle {
            node: Rc::downgrade(&node),
            row_uuid: row,
            tx_id,
            local_tier: DurabilityTier::Global,
        })
    }

    pub(super) fn accept_subscriber(
        &self,
        transport: Box<dyn Transport>,
        identity: AuthorId,
    ) -> Rc<LocalMutex<PeerConnection<RocksDbStorage>>> {
        self.server.accept_subscriber(transport, identity)
    }

    pub(super) fn accept_subscriber_with_trust(
        &self,
        transport: Box<dyn Transport>,
        identity: AuthorId,
        trust: CommitUnitTrust,
    ) -> Rc<LocalMutex<PeerConnection<RocksDbStorage>>> {
        self.server
            .accept_subscriber_with_trust(transport, identity, trust)
    }

    pub(super) fn accept_subscriber_with_claims(
        &self,
        transport: Box<dyn Transport>,
        identity: AuthorId,
        claims: BTreeMap<String, Value>,
    ) -> Rc<LocalMutex<PeerConnection<RocksDbStorage>>> {
        self.server
            .accept_subscriber_with_claims(transport, identity, claims)
    }

    pub(super) fn accept_subscriber_with_resume(
        &self,
        transport: Box<dyn Transport>,
        identity: AuthorId,
        cursor: ResumeCursor,
    ) -> Rc<LocalMutex<PeerConnection<RocksDbStorage>>> {
        self.server
            .accept_subscriber_with_resume(transport, identity, cursor)
    }

    pub(super) fn tick(&self) -> Result<(), Error> {
        block_on(self.server.tick()).map(|_| ())
    }

    pub(super) fn exclusive_tx(&self) -> Result<CoreExclusiveTx<'_>, Error> {
        let tx_id = OpenTransactionId::new();
        block_on(self.server.node().borrow_mut().open_exclusive(tx_id))?;
        Ok(CoreExclusiveTx {
            core: self,
            tx_id,
            has_reads: Cell::new(false),
        })
    }

    pub(super) fn publish_schema(&self, schema: SchemaVersion) -> Result<Vec<SyncMessage>, Error> {
        let node = self.server.node();
        let outcome = block_on(node.borrow_mut().apply_trusted_catalogue_message(
            SyncMessage::PublishSchema {
                author: self.author,
                schema: Box::new(schema),
            },
        ))?;
        block_on(node.borrow_mut().persist_and_settle_outcome(outcome)).map_err(Into::into)
    }

    pub(super) fn publish_schema_with_lens(
        &self,
        catalogue_seq: u64,
        publication: SchemaLineagePublication,
    ) -> Result<Vec<SyncMessage>, Error> {
        let node = self.server.node();
        let outcome = block_on(node.borrow_mut().apply_trusted_catalogue_message(
            SyncMessage::PublishSchemaWithLens {
                author: self.author,
                catalogue_seq,
                publication: Box::new(publication),
            },
        ))?;
        block_on(node.borrow_mut().persist_and_settle_outcome(outcome)).map_err(Into::into)
    }

    pub(super) fn set_current_write_schema(
        &self,
        pointer: CurrentWriteSchema,
    ) -> Result<Vec<SyncMessage>, Error> {
        let node = self.server.node();
        let outcome = block_on(node.borrow_mut().apply_trusted_catalogue_message(
            SyncMessage::SetCurrentWriteSchema {
                author: self.author,
                pointer,
            },
        ))?;
        block_on(node.borrow_mut().persist_and_settle_outcome(outcome)).map_err(Into::into)
    }
}

pub(super) struct CoreExclusiveTx<'a> {
    core: &'a CoreDb,
    tx_id: OpenTransactionId,
    has_reads: Cell<bool>,
}

impl CoreExclusiveTx<'_> {
    pub(super) fn read(&self, table: &str, row: RowUuid) -> Result<Option<RowCells>, Error> {
        self.has_reads.set(true);
        block_on(
            self.core
                .server
                .node()
                .borrow_mut()
                .tx_read(self.tx_id, table, row),
        )
        .map_err(Into::into)
    }

    pub(super) fn insert_with_id(
        &self,
        table: &str,
        row: RowUuid,
        cells: RowCells,
    ) -> Result<(), Error> {
        block_on(
            self.core
                .server
                .node()
                .borrow_mut()
                .tx_write(self.tx_id, table, row, cells, None),
        )
        .map_err(Into::into)
    }

    pub(super) fn update(&self, table: &str, row: RowUuid, patch: RowCells) -> Result<(), Error> {
        let mut cells = self.read(table, row)?.unwrap_or_default();
        cells.extend(patch);
        self.insert_with_id(table, row, cells)
    }

    pub(super) fn commit(self) -> Result<TxId, Error> {
        let node = self.core.server.node();
        if self.has_reads.get() && node.borrow().open_exclusive_snapshot_moved(self.tx_id)? {
            node.borrow_mut().abandon_tx(self.tx_id)?;
            return Err(write_rejected(
                self.tx_id,
                RejectionReason::ExclusiveConflict,
            ));
        }
        let (published, unit) = block_on(node.borrow_mut().commit_exclusive(
            self.tx_id,
            self.core.author,
            self.core.next_now_ms(),
        ))?;
        let tx_id = block_on(node.borrow_mut().persist_and_settle_transaction(published))?;
        let SyncMessage::CommitUnit { tx, versions } = unit else {
            return Err(Error::new(
                ErrorCode::Protocol,
                "commit_exclusive must yield a CommitUnit",
            ));
        };
        let outcome = block_on(
            node.borrow_mut()
                .finalize_local_exclusive_commit(tx, versions),
        )?;
        let fate = block_on(node.borrow_mut().persist_and_settle_outcome(outcome))?;
        if let Fate::Rejected(reason) = fate {
            return Err(write_rejected(tx_id, reason));
        }
        self.core.server.mark_subscriber_connections_dirty();
        Ok(tx_id)
    }
}

/// Commit a row on an authority node and confirm it reached Global, so the
/// serving path ships it.
pub(super) fn seed(db: &CoreDb, table: &str, cells: RowCells) -> RowUuid {
    let write = db.insert(table, cells).unwrap();
    block_on(write.wait(DurabilityTier::Global)).unwrap();
    write.row_uuid()
}

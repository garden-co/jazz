use std::collections::BTreeMap;

use futures_util::{FutureExt, StreamExt};
use jazz::abi::{
    ABI_FEATURE_NONE, ABI_VERSION, AbiResumeStatus, AbiRowBatch, AbiSubscriptionStreamChunk,
    AbiTransportDiagnostics, AbiWriteRejection, DbIdentityPayload, Event, EventBudget, EventKind,
    FrameBudget, Handle, MemoryStorageConfig, OpenDbConfig, SubscriberTransportHints, TickStats,
    TransportDirection, WriteStateTarget,
};
use jazz::abi_runtime::{AbiCallResult, AbiRuntime, AbiSubscriptionStream};
use jazz::db::{DbIdentity, RowCells};
use jazz::groove::records::Value;
use jazz::groove::schema::ColumnType;
use jazz::ids::{AuthorId, NodeUuid, RowUuid};
use jazz::query::Query;
use jazz::schema::{ColumnSchema, JazzSchema, Policy, TableSchema};
use jazz::tx::{Fate, RejectionReason};
use jazz_server::{
    DrainState, HealthStatus, InMemoryServerShell, InMemoryServerShellConfig, ShellError,
};

fn todos_schema() -> JazzSchema {
    JazzSchema::new([TableSchema::new(
        "todos",
        [
            ColumnSchema::new("title", ColumnType::String),
            ColumnSchema::new("done", ColumnType::Bool),
        ],
    )])
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
    .with_write_policy(Policy::owner_only("todos", "owner"))])
}

fn identity(node: u8, author: u8) -> DbIdentity {
    DbIdentity {
        node: NodeUuid::from_bytes([node; 16]),
        author: AuthorId::from_bytes([author; 16]),
    }
}

fn todo_cells(title: &str, done: bool) -> RowCells {
    BTreeMap::from([
        ("title".to_owned(), Value::String(title.to_owned())),
        ("done".to_owned(), Value::Bool(done)),
    ])
}

fn owned_todo_cells(title: &str, done: bool, owner: AuthorId) -> RowCells {
    BTreeMap::from([
        ("title".to_owned(), Value::String(title.to_owned())),
        ("done".to_owned(), Value::Bool(done)),
        ("owner".to_owned(), Value::Uuid(owner.0)),
    ])
}

fn expect_ack(result: AbiCallResult) {
    assert_eq!(result, AbiCallResult::Ack);
}

fn drain_events(runtime: &mut AbiRuntime, runtime_handle: Handle, request_id: u64) -> Vec<Event> {
    let AbiCallResult::Events { events } = runtime.events_poll(
        Some(request_id),
        runtime_handle,
        EventBudget {
            max_events: 128,
            max_bytes: u64::MAX,
        },
    ) else {
        panic!("expected event poll");
    };
    events
}

fn open_client_runtime(schema: JazzSchema, client: DbIdentity) -> (AbiRuntime, Handle, Handle) {
    let mut runtime = AbiRuntime::new();
    let AbiCallResult::RuntimeInitialized {
        runtime: runtime_handle,
    } = runtime.runtime_init(Some(10), ABI_VERSION, ABI_FEATURE_NONE)
    else {
        panic!("expected runtime init");
    };
    drain_events(&mut runtime, runtime_handle, 11);

    expect_ack(runtime.storage_open_memory(
        Some(12),
        postcard::to_allocvec(&MemoryStorageConfig::default()).unwrap(),
    ));
    let storage = drain_events(&mut runtime, runtime_handle, 13)
        .into_iter()
        .find_map(|event| match event.kind {
            EventKind::StorageOpened { storage, .. } => Some(storage),
            _ => None,
        })
        .unwrap();

    expect_ack(
        runtime.db_open_memory(
            Some(14),
            storage,
            postcard::to_allocvec(&schema).unwrap(),
            postcard::to_allocvec(&OpenDbConfig {
                identity: DbIdentityPayload::from(client),
                row_id_seed: Some(0xc1),
                history_complete: false,
            })
            .unwrap(),
        ),
    );
    let db = drain_events(&mut runtime, runtime_handle, 15)
        .into_iter()
        .find_map(|event| match event.kind {
            EventKind::DbOpened { db, .. } => Some(db),
            _ => None,
        })
        .unwrap();

    (runtime, runtime_handle, db)
}

struct TodosSubscription {
    stream: AbiSubscriptionStream,
    rows: BTreeMap<RowUuid, (String, bool)>,
}

impl TodosSubscription {
    fn ingest_available(&mut self) {
        while let Some(chunk) = self.stream.next().now_or_never() {
            let Some(chunk) = chunk else {
                break;
            };
            match chunk.unwrap() {
                AbiSubscriptionStreamChunk::Snapshot(snapshot) => {
                    self.rows.clear();
                    ingest_batches(&mut self.rows, &snapshot.rows);
                }
                AbiSubscriptionStreamChunk::Delta(chunk) => {
                    for removed in chunk.delta.removed {
                        self.rows.remove(&removed.row_id);
                    }
                    ingest_batches(&mut self.rows, &chunk.delta.added);
                    ingest_batches(&mut self.rows, &chunk.delta.updated);
                }
            }
        }
    }

    fn fields(&mut self) -> Vec<(String, bool)> {
        self.ingest_available();
        let mut fields = self.rows.values().cloned().collect::<Vec<_>>();
        fields.sort();
        fields
    }
}

fn ingest_batches(rows: &mut BTreeMap<RowUuid, (String, bool)>, batches: &[AbiRowBatch]) {
    for batch in batches {
        assert_eq!(batch.table, "todos");
        for row in &batch.rows {
            let record = batch.descriptor.bind(&row.raw);
            let Value::Nullable(Some(title)) = record.get("user_title").unwrap() else {
                panic!("expected nullable title");
            };
            let Value::String(title) = title.as_ref() else {
                panic!("expected string title");
            };
            let Value::Nullable(Some(done)) = record.get("user_done").unwrap() else {
                panic!("expected nullable done");
            };
            let Value::Bool(done) = done.as_ref() else {
                panic!("expected bool done");
            };
            rows.insert(row.row_id, (title.clone(), *done));
        }
    }
}

fn prepare_todos_subscription(
    runtime: &mut AbiRuntime,
    runtime_handle: Handle,
    db: Handle,
) -> TodosSubscription {
    expect_ack(runtime.db_prepare_query(
        Some(20),
        db,
        postcard::to_allocvec(&Query::from("todos")).unwrap(),
    ));
    let query = drain_events(runtime, runtime_handle, 21)
        .into_iter()
        .find_map(|event| match event.kind {
            EventKind::QueryPrepared { query, .. } => Some(query),
            _ => None,
        })
        .unwrap();

    let mut subscription = TodosSubscription {
        stream: runtime
            .db_subscribe_stream(Some(22), db, query, Default::default())
            .unwrap(),
        rows: BTreeMap::new(),
    };
    subscription.ingest_available();
    subscription
}

fn attach_upstream(runtime: &mut AbiRuntime, runtime_handle: Handle, db: Handle) -> Handle {
    expect_ack(runtime.transport_attach(Some(30), db, TransportDirection::Upstream, Vec::new()));
    drain_events(runtime, runtime_handle, 31)
        .into_iter()
        .find_map(|event| match event.kind {
            EventKind::TransportAttached { transport, .. } => Some(transport),
            _ => None,
        })
        .unwrap()
}

fn transport_diagnostics(
    runtime: &mut AbiRuntime,
    runtime_handle: Handle,
    transport: Handle,
    request_id: u64,
) -> AbiTransportDiagnostics {
    expect_ack(runtime.transport_diagnostics(Some(request_id), transport));
    drain_events(runtime, runtime_handle, request_id + 1)
        .into_iter()
        .find_map(|event| match event.kind {
            EventKind::TransportDiagnostics { payload } => {
                Some(postcard::from_bytes::<AbiTransportDiagnostics>(&payload).unwrap())
            }
            _ => None,
        })
        .unwrap()
}

fn take_client_frames(
    runtime: &mut AbiRuntime,
    runtime_handle: Handle,
    transport: Handle,
    request_id: u64,
) -> Vec<Vec<u8>> {
    expect_ack(runtime.transport_recv_wire_frame(
        Some(request_id),
        transport,
        FrameBudget {
            max_frames: 16,
            max_bytes: u64::MAX,
        },
    ));
    drain_events(runtime, runtime_handle, request_id + 1)
        .into_iter()
        .flat_map(|event| match event.kind {
            EventKind::WireFrames { frames, .. } => frames,
            _ => Vec::new(),
        })
        .collect()
}

fn pump(
    runtime: &mut AbiRuntime,
    runtime_handle: Handle,
    db: Handle,
    transport: Handle,
    server: &mut InMemoryServerShell,
    session: jazz_server::ServerSession,
    request_id_base: u64,
) -> bool {
    pump_counting_server_egress_bytes(
        runtime,
        runtime_handle,
        db,
        transport,
        server,
        session,
        request_id_base,
    )
    .0
}

fn pump_counting_server_egress_bytes(
    runtime: &mut AbiRuntime,
    runtime_handle: Handle,
    db: Handle,
    transport: Handle,
    server: &mut InMemoryServerShell,
    session: jazz_server::ServerSession,
    request_id_base: u64,
) -> (bool, usize) {
    let mut saw_client_inbound = false;
    let mut server_egress_bytes = 0;
    for offset in 0..64 {
        let request_id = request_id_base + offset * 10;
        expect_ack(runtime.db_drive_tick(Some(request_id), db));
        let client_frames = take_client_frames(runtime, runtime_handle, transport, request_id + 1);
        server.receive_frames(session, client_frames).unwrap();

        server.tick().unwrap();
        let server_frames = server.take_frames(session).unwrap();
        server_egress_bytes += server_frames.iter().map(Vec::len).sum::<usize>();
        for frame in server_frames {
            expect_ack(runtime.transport_send_wire_frame(None, transport, frame));
        }

        expect_ack(runtime.db_drive_tick(Some(request_id + 3), db));
        saw_client_inbound |= drain_events(runtime, runtime_handle, request_id + 4)
            .into_iter()
            .any(|event| {
                matches!(
                    event.kind,
                    EventKind::TickDone {
                        stats: TickStats { inbound, .. },
                    } if inbound > 0
                )
            });
    }
    (saw_client_inbound, server_egress_bytes)
}

fn pump_until_write_rejected(
    runtime: &mut AbiRuntime,
    runtime_handle: Handle,
    db: Handle,
    transport: Handle,
    server: &mut InMemoryServerShell,
    session: jazz_server::ServerSession,
    write: Handle,
) {
    for attempt in 0..16 {
        pump(
            runtime,
            runtime_handle,
            db,
            transport,
            server,
            session,
            1000 + attempt * 1000,
        );
        expect_ack(
            runtime.db_get_write_state(Some(9000 + attempt * 2), WriteStateTarget::Handle(write)),
        );
        let rejected = drain_events(runtime, runtime_handle, 9001 + attempt * 2)
            .into_iter()
            .any(|event| {
                matches!(
                    event.kind,
                    EventKind::WriteState { state, .. }
                        if state.fate == Fate::Rejected(RejectionReason::AuthorizationDenied)
                )
            });
        if rejected {
            return;
        }
    }
    panic!("write was not rejected after public frame pump");
}

#[test]
fn in_memory_server_shell_round_trips_structured_write_rejection_details() {
    let schema = owner_write_schema();
    let server_identity = identity(0x5e, 0x01);
    let author_a = identity(0xc1, 0xa1);
    let author_b = AuthorId::from_bytes([0xb2; 16]);
    let mut server = InMemoryServerShell::start(
        InMemoryServerShellConfig::new(schema.clone(), server_identity).with_row_id_seed(0x5e),
    )
    .unwrap();

    let (mut client_runtime, client_runtime_handle, client_db) =
        open_client_runtime(schema, author_a);
    let client_transport = attach_upstream(&mut client_runtime, client_runtime_handle, client_db);
    let session = server.accept_subscriber_session(author_a.author).unwrap();

    expect_ack(client_runtime.db_insert_with_id(
        Some(40),
        client_db,
        "todos".to_owned(),
        RowUuid::from_bytes([0x41; 16]),
        postcard::to_allocvec(&owned_todo_cells("wrong owner", false, author_b)).unwrap(),
    ));
    let write = drain_events(&mut client_runtime, client_runtime_handle, 41)
        .into_iter()
        .find_map(|event| match event.kind {
            EventKind::WriteStarted { write, .. } => Some(write),
            _ => None,
        })
        .unwrap();

    pump_until_write_rejected(
        &mut client_runtime,
        client_runtime_handle,
        client_db,
        client_transport,
        &mut server,
        session,
        write,
    );

    expect_ack(client_runtime.db_get_write_state(Some(9500), WriteStateTarget::Handle(write)));
    let state = drain_events(&mut client_runtime, client_runtime_handle, 9501)
        .into_iter()
        .find_map(|event| match event.kind {
            EventKind::WriteState { state, .. } => Some(state),
            _ => None,
        })
        .unwrap();

    assert_eq!(
        state.fate,
        Fate::Rejected(RejectionReason::AuthorizationDenied)
    );
    assert_eq!(
        state.rejection,
        Some(AbiWriteRejection::AuthorizationDenied)
    );
}

#[test]
fn in_memory_server_shell_syncs_rows_to_client_watch() {
    let schema = todos_schema();
    let server_identity = identity(0x5e, 0x01);
    let client_identity = identity(0xc1, 0xa1);
    let mut server = InMemoryServerShell::start(
        InMemoryServerShellConfig::new(schema.clone(), server_identity).with_row_id_seed(0x5e),
    )
    .unwrap();

    server
        .seed_row_with_id(
            "todos",
            RowUuid::from_bytes([0x31; 16]),
            todo_cells("from server", false),
        )
        .unwrap();

    let (mut client_runtime, client_runtime_handle, client_db) =
        open_client_runtime(schema, client_identity);
    let mut subscription =
        prepare_todos_subscription(&mut client_runtime, client_runtime_handle, client_db);
    let client_transport = attach_upstream(&mut client_runtime, client_runtime_handle, client_db);
    let session = server
        .accept_subscriber_session(client_identity.author)
        .unwrap();
    let metrics = server.metrics_snapshot();
    assert_eq!(metrics.active_sessions, 1);
    assert_eq!(metrics.total_sessions, 1);
    assert_eq!(metrics.rejected_sessions, 0);
    assert_eq!(server.health_snapshot().status, HealthStatus::Ready);

    assert!(pump(
        &mut client_runtime,
        client_runtime_handle,
        client_db,
        client_transport,
        &mut server,
        session,
        100,
    ));
    let metrics = server.metrics_snapshot();
    assert!(metrics.ticks > 0);
    assert!(metrics.frames_received > 0);
    assert!(metrics.frames_sent > 0);
    assert!(metrics.bytes_received > 0);
    assert!(metrics.bytes_sent > 0);
    assert!(metrics.tick_outbound > 0);
    assert_eq!(
        subscription.fields(),
        vec![("from server".to_owned(), false)]
    );

    server
        .seed_row_with_id(
            "todos",
            RowUuid::from_bytes([0x32; 16]),
            todo_cells("second", true),
        )
        .unwrap();
    assert!(pump(
        &mut client_runtime,
        client_runtime_handle,
        client_db,
        client_transport,
        &mut server,
        session,
        1000,
    ));
    assert_eq!(
        subscription.fields(),
        vec![
            ("from server".to_owned(), false),
            ("second".to_owned(), true),
        ]
    );
}

#[test]
fn sessioned_transport_smoke_detach_reconnects_and_resumes_through_public_frame_pump() {
    let schema = todos_schema();
    let server_identity = identity(0x5e, 0x01);
    let client_identity = identity(0xc1, 0xa1);
    let mut server = InMemoryServerShell::start(
        InMemoryServerShellConfig::new(schema.clone(), server_identity).with_row_id_seed(0x5e),
    )
    .unwrap();

    server
        .seed_row_with_id(
            "todos",
            RowUuid::from_bytes([0x31; 16]),
            todo_cells("from server", false),
        )
        .unwrap();
    server
        .seed_row_with_id(
            "todos",
            RowUuid::from_bytes([0x32; 16]),
            todo_cells("second", true),
        )
        .unwrap();

    let (mut client_runtime, client_runtime_handle, client_db) =
        open_client_runtime(schema.clone(), client_identity);
    let mut subscription =
        prepare_todos_subscription(&mut client_runtime, client_runtime_handle, client_db);
    let mut client_transport =
        attach_upstream(&mut client_runtime, client_runtime_handle, client_db);
    let client_fresh_diagnostics = transport_diagnostics(
        &mut client_runtime,
        client_runtime_handle,
        client_transport,
        90,
    );
    assert_eq!(client_fresh_diagnostics.transport, client_transport);
    assert_eq!(
        client_fresh_diagnostics.resume_status,
        AbiResumeStatus::Fresh
    );
    assert_eq!(client_fresh_diagnostics.inbound_queue_depth, 0);
    assert_eq!(client_fresh_diagnostics.outbound_queue_depth, 0);

    let session = server
        .accept_subscriber_session(client_identity.author)
        .unwrap();
    let fresh_diagnostics = server.session_diagnostics(session).unwrap();
    assert_eq!(fresh_diagnostics.resume_status, AbiResumeStatus::Fresh);

    let (saw_initial, initial_server_egress_bytes) = pump_counting_server_egress_bytes(
        &mut client_runtime,
        client_runtime_handle,
        client_db,
        client_transport,
        &mut server,
        session,
        100,
    );
    assert!(saw_initial);
    assert_eq!(
        subscription.fields(),
        vec![
            ("from server".to_owned(), false),
            ("second".to_owned(), true),
        ]
    );

    let resume = server.disconnect_session_for_resume(session).unwrap();
    assert_eq!(server.metrics_snapshot().active_sessions, 0);
    expect_ack(client_runtime.transport_detach(Some(500), client_transport));
    assert!(
        drain_events(&mut client_runtime, client_runtime_handle, 501)
            .into_iter()
            .any(|event| matches!(event.kind, EventKind::TransportClosed { transport, .. } if transport == client_transport))
    );
    server
        .seed_row_with_id(
            "todos",
            RowUuid::from_bytes([0x33; 16]),
            todo_cells("third while away", false),
        )
        .unwrap();
    client_transport = attach_upstream(&mut client_runtime, client_runtime_handle, client_db);
    let client_reconnect_diagnostics = transport_diagnostics(
        &mut client_runtime,
        client_runtime_handle,
        client_transport,
        510,
    );
    assert_eq!(client_reconnect_diagnostics.transport, client_transport);
    assert_ne!(
        client_reconnect_diagnostics.session_id,
        client_fresh_diagnostics.session_id
    );
    assert!(client_reconnect_diagnostics.epoch > client_fresh_diagnostics.epoch);
    assert_eq!(
        client_reconnect_diagnostics.resume_status,
        AbiResumeStatus::Fresh
    );

    let resumed_session = server.resume_subscriber_session(resume).unwrap();
    let resumed_diagnostics = server.session_diagnostics(resumed_session).unwrap();
    assert_eq!(resumed_diagnostics.resume_status, AbiResumeStatus::Resumed);
    assert_ne!(resumed_diagnostics.session_id, fresh_diagnostics.session_id);
    assert!(resumed_diagnostics.epoch > fresh_diagnostics.epoch);
    // TODO(sessioned-transport): stale/wrong-identity rejection is proven for
    // process-local subscriber resume tokens below; portable WireFrame-level
    // resume credentials need protocol semantics before a network canary can
    // assert cross-runtime rejection.
    let metrics = server.metrics_snapshot();
    assert_eq!(metrics.active_sessions, 1);
    assert_eq!(metrics.total_sessions, 2);

    let (_saw_resume, resume_server_egress_bytes) = pump_counting_server_egress_bytes(
        &mut client_runtime,
        client_runtime_handle,
        client_db,
        client_transport,
        &mut server,
        resumed_session,
        1000,
    );
    assert_eq!(
        subscription.fields(),
        vec![
            ("from server".to_owned(), false),
            ("second".to_owned(), true),
            ("third while away".to_owned(), false),
        ]
    );
    assert!(
        resume_server_egress_bytes < initial_server_egress_bytes,
        "resume egress ({resume_server_egress_bytes}) should be smaller than initial two-row sync ({initial_server_egress_bytes})"
    );

    let fresh_identity = identity(0xc2, 0xa2);
    let (mut fresh_runtime, fresh_runtime_handle, fresh_db) =
        open_client_runtime(schema, fresh_identity);
    let mut fresh_subscription =
        prepare_todos_subscription(&mut fresh_runtime, fresh_runtime_handle, fresh_db);
    let fresh_transport = attach_upstream(&mut fresh_runtime, fresh_runtime_handle, fresh_db);
    let fresh_session = server
        .accept_subscriber_session(fresh_identity.author)
        .unwrap();
    let (saw_fresh, fresh_server_egress_bytes) = pump_counting_server_egress_bytes(
        &mut fresh_runtime,
        fresh_runtime_handle,
        fresh_db,
        fresh_transport,
        &mut server,
        fresh_session,
        2000,
    );
    assert!(saw_fresh);
    assert_eq!(
        fresh_subscription.fields(),
        vec![
            ("from server".to_owned(), false),
            ("second".to_owned(), true),
            ("third while away".to_owned(), false),
        ]
    );
    assert!(
        resume_server_egress_bytes < fresh_server_egress_bytes,
        "resume egress ({resume_server_egress_bytes}) should be smaller than fresh three-row sync ({fresh_server_egress_bytes})"
    );
}

#[test]
fn in_memory_server_shell_reports_live_drain_health_and_rejections() {
    let schema = todos_schema();
    let server_identity = identity(0x5e, 0x01);
    let first_identity = identity(0xc1, 0xa1);
    let rejected_identity = identity(0xc2, 0xa2);
    let mut server = InMemoryServerShell::start(
        InMemoryServerShellConfig::new(schema, server_identity).with_row_id_seed(0x5e),
    )
    .unwrap();

    assert_eq!(server.drain_state(), DrainState::Running);
    assert_eq!(server.health_snapshot().status, HealthStatus::Ready);
    assert_eq!(server.metrics_snapshot().active_sessions, 0);

    let session = server
        .accept_subscriber_session(first_identity.author)
        .unwrap();
    server.begin_drain();

    let health = server.health_snapshot();
    assert_eq!(health.status, HealthStatus::Draining);
    assert_eq!(health.drain_state, DrainState::Draining);

    let error = server
        .accept_subscriber_session(rejected_identity.author)
        .unwrap_err();
    assert_eq!(
        error,
        ShellError::SessionRejected {
            drain_state: DrainState::Draining,
        }
    );
    let metrics = server.metrics_snapshot();
    assert_eq!(metrics.active_sessions, 1);
    assert_eq!(metrics.total_sessions, 1);
    assert_eq!(metrics.rejected_sessions, 1);

    server.close_session(session).unwrap();
    assert_eq!(server.drain_state(), DrainState::Drained);
    let health = server.health_snapshot();
    assert_eq!(health.status, HealthStatus::Draining);
    assert_eq!(health.drain_state, DrainState::Drained);
    assert_eq!(server.metrics_snapshot().active_sessions, 0);

    let error = server
        .accept_subscriber_session(rejected_identity.author)
        .unwrap_err();
    assert_eq!(
        error,
        ShellError::SessionRejected {
            drain_state: DrainState::Drained,
        }
    );
    assert_eq!(server.metrics_snapshot().rejected_sessions, 2);
}

#[test]
fn abi_runtime_rejects_invalid_subscriber_resume_token() {
    let schema = todos_schema();
    let server_identity = identity(0x5e, 0x01);
    let mut runtime = AbiRuntime::new();
    let AbiCallResult::RuntimeInitialized {
        runtime: runtime_handle,
    } = runtime.runtime_init(Some(10), ABI_VERSION, ABI_FEATURE_NONE)
    else {
        panic!("expected runtime init");
    };
    drain_events(&mut runtime, runtime_handle, 11);

    expect_ack(runtime.storage_open_memory(
        Some(12),
        postcard::to_allocvec(&MemoryStorageConfig::default()).unwrap(),
    ));
    let storage = drain_events(&mut runtime, runtime_handle, 13)
        .into_iter()
        .find_map(|event| match event.kind {
            EventKind::StorageOpened { storage, .. } => Some(storage),
            _ => None,
        })
        .unwrap();

    expect_ack(
        runtime.db_open_memory(
            Some(14),
            storage,
            postcard::to_allocvec(&schema).unwrap(),
            postcard::to_allocvec(&OpenDbConfig {
                identity: DbIdentityPayload::from(server_identity),
                row_id_seed: Some(0x5e),
                history_complete: true,
            })
            .unwrap(),
        ),
    );
    let db = drain_events(&mut runtime, runtime_handle, 15)
        .into_iter()
        .find_map(|event| match event.kind {
            EventKind::DbOpened { db, .. } => Some(db),
            _ => None,
        })
        .unwrap();

    let result = runtime.transport_attach(
        Some(16),
        db,
        TransportDirection::Subscriber,
        postcard::to_allocvec(&SubscriberTransportHints {
            identity: server_identity.author,
            resume_token: Some(42),
            claims: BTreeMap::new(),
        })
        .unwrap(),
    );
    let AbiCallResult::Error { error } = result else {
        panic!("expected invalid resume token error");
    };
    assert!(
        error
            .message
            .contains("invalid subscriber resume token: 42")
    );
    assert!(
        !drain_events(&mut runtime, runtime_handle, 17)
            .into_iter()
            .any(|event| matches!(event.kind, EventKind::TransportDiagnostics { .. }))
    );
}

#[test]
fn abi_runtime_wrong_subscriber_resume_identity_does_not_consume_token() {
    let schema = todos_schema();
    let server_identity = identity(0x5e, 0x01);
    let client_identity = identity(0xc1, 0xa1);
    let wrong_identity = identity(0xc2, 0xa2);
    let mut runtime = AbiRuntime::new();
    let AbiCallResult::RuntimeInitialized {
        runtime: runtime_handle,
    } = runtime.runtime_init(Some(10), ABI_VERSION, ABI_FEATURE_NONE)
    else {
        panic!("expected runtime init");
    };
    drain_events(&mut runtime, runtime_handle, 11);

    expect_ack(runtime.storage_open_memory(
        Some(12),
        postcard::to_allocvec(&MemoryStorageConfig::default()).unwrap(),
    ));
    let storage = drain_events(&mut runtime, runtime_handle, 13)
        .into_iter()
        .find_map(|event| match event.kind {
            EventKind::StorageOpened { storage, .. } => Some(storage),
            _ => None,
        })
        .unwrap();

    expect_ack(
        runtime.db_open_memory(
            Some(14),
            storage,
            postcard::to_allocvec(&schema).unwrap(),
            postcard::to_allocvec(&OpenDbConfig {
                identity: DbIdentityPayload::from(server_identity),
                row_id_seed: Some(0x5e),
                history_complete: true,
            })
            .unwrap(),
        ),
    );
    let db = drain_events(&mut runtime, runtime_handle, 15)
        .into_iter()
        .find_map(|event| match event.kind {
            EventKind::DbOpened { db, .. } => Some(db),
            _ => None,
        })
        .unwrap();

    expect_ack(
        runtime.transport_attach(
            Some(16),
            db,
            TransportDirection::Subscriber,
            postcard::to_allocvec(&SubscriberTransportHints {
                identity: client_identity.author,
                resume_token: None,
                claims: BTreeMap::new(),
            })
            .unwrap(),
        ),
    );
    let transport = drain_events(&mut runtime, runtime_handle, 17)
        .into_iter()
        .find_map(|event| match event.kind {
            EventKind::TransportAttached { transport, .. } => Some(transport),
            _ => None,
        })
        .unwrap();

    let (result, resume_token) = runtime.transport_detach_for_resume(Some(18), transport);
    expect_ack(result);
    drain_events(&mut runtime, runtime_handle, 19);
    let resume_token = resume_token.unwrap();

    let result = runtime.transport_attach(
        Some(20),
        db,
        TransportDirection::Subscriber,
        postcard::to_allocvec(&SubscriberTransportHints {
            identity: wrong_identity.author,
            resume_token: Some(resume_token),
            claims: BTreeMap::new(),
        })
        .unwrap(),
    );
    let AbiCallResult::Error { error } = result else {
        panic!("expected wrong identity resume error");
    };
    assert!(error.message.contains("does not match identity"));

    expect_ack(
        runtime.transport_attach(
            Some(22),
            db,
            TransportDirection::Subscriber,
            postcard::to_allocvec(&SubscriberTransportHints {
                identity: client_identity.author,
                resume_token: Some(resume_token),
                claims: BTreeMap::new(),
            })
            .unwrap(),
        ),
    );
    assert!(
        drain_events(&mut runtime, runtime_handle, 23)
            .into_iter()
            .any(|event| matches!(event.kind, EventKind::TransportAttached { .. }))
    );
}

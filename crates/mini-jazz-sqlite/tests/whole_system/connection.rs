use super::support::{FixtureRuntimeExt, Harness};
use mini_jazz_sqlite::connection::{
    in_memory_connection_pair, DownstreamEndpoint, UpstreamEndpoint,
};
use mini_jazz_sqlite::connection::{DownstreamConnectionManager, UpstreamConnectionManager};
use mini_jazz_sqlite::protocol::{
    ClientHello, ClientMessage, CloseReason, MessageId, ProtocolCapabilities, ProtocolError,
    ProtocolVersion, ReplayCursor, ReplaySubscription, RetryHint, ServerHello, ServerMessage,
    SessionId, SettlementTier, SubscriptionId,
};
use mini_jazz_sqlite::session::{DownstreamSession, UpstreamSession};
use mini_jazz_sqlite::{BuiltQuery, QueryCondition, QueryConditionOp};
use serde_json::json;

#[test]
fn connection_subscribe_delivers_initial_query_and_settled() {
    let harness = Harness::new();
    let mut tab = harness.memory("alice-tab", "alice").unwrap();
    let mut worker = harness.memory("alice-worker", "alice").unwrap();
    worker.create_project("project-1", "Launch notes").unwrap();
    worker
        .create_todo("todo-1", "Draft protocol", false, "project-1")
        .unwrap();
    worker
        .create_todo("todo-2", "Archived thought", true, "project-1")
        .unwrap();

    let query = open_todos_query();
    let subscription_id = SubscriptionId::new("open-todos");
    let (mut downstream_conn, mut upstream_conn) = in_memory_connection_pair();
    let mut downstream = DownstreamSession::new(
        "tab-session",
        "alice-tab",
        tab.local_schema_fingerprint(),
        tab.local_policy_fingerprint(),
    );
    let mut upstream = UpstreamSession::new(
        "worker-session",
        "alice-worker",
        worker.local_schema_fingerprint(),
        worker.local_policy_fingerprint(),
    );

    downstream.open(&mut downstream_conn).unwrap();
    upstream.pump(&mut worker, &mut upstream_conn).unwrap();
    downstream.pump(&mut tab, &mut downstream_conn).unwrap();

    downstream
        .subscribe(
            &mut downstream_conn,
            subscription_id.clone(),
            query.clone(),
            SettlementTier::Local,
        )
        .unwrap();
    upstream.pump(&mut worker, &mut upstream_conn).unwrap();
    downstream.pump(&mut tab, &mut downstream_conn).unwrap();

    assert!(downstream.is_settled(&subscription_id, SettlementTier::Local));
    upstream.pump(&mut worker, &mut upstream_conn).unwrap();
    assert_eq!(
        upstream.last_acknowledged_cursor(&subscription_id),
        Some(ReplayCursor(1))
    );
    assert_eq!(row_ids(tab.query(query).unwrap()), vec!["todo-1"]);
}

#[test]
fn connection_rejects_unsupported_edge_subscription_tier() {
    let harness = Harness::new();
    let mut tab = harness.memory("alice-tab", "alice").unwrap();
    let mut worker = harness.memory("alice-worker", "alice").unwrap();
    worker.create_project("project-1", "Launch notes").unwrap();
    worker
        .create_todo("todo-1", "Draft protocol", false, "project-1")
        .unwrap();

    let query = open_todos_query();
    let subscription_id = SubscriptionId::new("open-todos");
    let (mut downstream_conn, mut upstream_conn) = in_memory_connection_pair();
    let mut downstream = DownstreamSession::new(
        "tab-session",
        "alice-tab",
        tab.local_schema_fingerprint(),
        tab.local_policy_fingerprint(),
    );
    let mut upstream = UpstreamSession::new(
        "worker-session",
        "alice-worker",
        worker.local_schema_fingerprint(),
        worker.local_policy_fingerprint(),
    );

    downstream.open(&mut downstream_conn).unwrap();
    upstream.pump(&mut worker, &mut upstream_conn).unwrap();
    downstream.pump(&mut tab, &mut downstream_conn).unwrap();
    downstream
        .subscribe(
            &mut downstream_conn,
            subscription_id.clone(),
            query.clone(),
            SettlementTier::Edge,
        )
        .unwrap();
    upstream.pump(&mut worker, &mut upstream_conn).unwrap();
    downstream.pump(&mut tab, &mut downstream_conn).unwrap();

    let error = downstream.last_error().expect("subscription is rejected");
    assert_eq!(error.code, "unsupported_settlement_tier");
    assert_eq!(error.subscription_id.as_ref(), Some(&subscription_id));
    assert_eq!(error.retry_hint, RetryHint::Retryable);
    assert!(!downstream.has_active_subscription(&subscription_id));
    assert!(row_ids(tab.query(query).unwrap()).is_empty());
    assert!(!downstream.is_settled(&subscription_id, SettlementTier::Local));
    assert!(!downstream.is_settled(&subscription_id, SettlementTier::Edge));
    assert!(!downstream.is_settled(&subscription_id, SettlementTier::Global));
}

#[test]
fn connection_new_subscription_data_invalidates_previous_settlement() {
    let harness = Harness::new();
    let mut tab = harness.memory("alice-tab", "alice").unwrap();
    let mut worker = harness.memory("alice-worker", "alice").unwrap();
    worker.create_project("project-1", "Launch notes").unwrap();
    worker
        .create_todo("todo-1", "Draft protocol", false, "project-1")
        .unwrap();

    let query = open_todos_query();
    let subscription_id = SubscriptionId::new("open-todos");
    let (mut downstream_conn, mut upstream_conn) = in_memory_connection_pair();
    let mut downstream = DownstreamSession::new(
        "tab-session",
        "alice-tab",
        tab.local_schema_fingerprint(),
        tab.local_policy_fingerprint(),
    );
    let mut upstream = UpstreamSession::new(
        "worker-session",
        "alice-worker",
        worker.local_schema_fingerprint(),
        worker.local_policy_fingerprint(),
    );

    downstream.open(&mut downstream_conn).unwrap();
    upstream.pump(&mut worker, &mut upstream_conn).unwrap();
    downstream.pump(&mut tab, &mut downstream_conn).unwrap();
    downstream
        .subscribe(
            &mut downstream_conn,
            subscription_id.clone(),
            query.clone(),
            SettlementTier::Local,
        )
        .unwrap();
    upstream.pump(&mut worker, &mut upstream_conn).unwrap();
    downstream.pump(&mut tab, &mut downstream_conn).unwrap();
    assert!(downstream.is_settled(&subscription_id, SettlementTier::Local));

    worker
        .create_todo("todo-2", "Invalidate settlement", false, "project-1")
        .unwrap();
    let bundle = worker.export_query(query.clone()).unwrap();
    upstream_conn.send_server_message(ServerMessage::Data {
        message_id: MessageId(2),
        subscription_id: Some(subscription_id.clone()),
        cursor: ReplayCursor(2),
        bundle,
    });
    downstream.pump(&mut tab, &mut downstream_conn).unwrap();

    assert_eq!(
        sorted_row_ids(tab.query(query).unwrap()),
        vec!["todo-1", "todo-2"]
    );
    assert!(!downstream.is_settled(&subscription_id, SettlementTier::Local));
}

#[test]
fn connection_replay_restores_active_subscription_after_reconnect() {
    let harness = Harness::new();
    let mut tab = harness.memory("alice-tab", "alice").unwrap();
    let mut worker = harness.memory("alice-worker", "alice").unwrap();
    worker.create_project("project-1", "Launch notes").unwrap();
    worker
        .create_todo("todo-1", "Draft protocol", false, "project-1")
        .unwrap();

    let query = open_todos_query();
    let subscription_id = SubscriptionId::new("open-todos");
    let (mut downstream_conn, mut upstream_conn) = in_memory_connection_pair();
    let mut downstream = DownstreamSession::new(
        "tab-session",
        "alice-tab",
        tab.local_schema_fingerprint(),
        tab.local_policy_fingerprint(),
    );
    let mut upstream = UpstreamSession::new(
        "worker-session",
        "alice-worker",
        worker.local_schema_fingerprint(),
        worker.local_policy_fingerprint(),
    );

    downstream.open(&mut downstream_conn).unwrap();
    upstream.pump(&mut worker, &mut upstream_conn).unwrap();
    downstream.pump(&mut tab, &mut downstream_conn).unwrap();
    downstream
        .subscribe(
            &mut downstream_conn,
            subscription_id.clone(),
            query.clone(),
            SettlementTier::Local,
        )
        .unwrap();
    upstream.pump(&mut worker, &mut upstream_conn).unwrap();
    downstream.pump(&mut tab, &mut downstream_conn).unwrap();
    assert_eq!(row_ids(tab.query(query.clone()).unwrap()), vec!["todo-1"]);

    worker
        .create_todo("todo-2", "Reconnect protocol", false, "project-1")
        .unwrap();

    let (mut downstream_conn, mut upstream_conn) = in_memory_connection_pair();
    let mut upstream = UpstreamSession::new(
        "worker-session-reconnected",
        "alice-worker",
        worker.local_schema_fingerprint(),
        worker.local_policy_fingerprint(),
    );
    downstream.open(&mut downstream_conn).unwrap();
    upstream.pump(&mut worker, &mut upstream_conn).unwrap();
    downstream.pump(&mut tab, &mut downstream_conn).unwrap();
    downstream.replay(&mut downstream_conn).unwrap();
    upstream.pump(&mut worker, &mut upstream_conn).unwrap();
    downstream.pump(&mut tab, &mut downstream_conn).unwrap();

    assert!(downstream.is_settled(&subscription_id, SettlementTier::Local));
    assert_eq!(
        sorted_row_ids(tab.query(query).unwrap()),
        vec!["todo-1", "todo-2"]
    );
}

#[test]
fn connection_close_drops_session_interest_without_deleting_cached_rows() {
    let harness = Harness::new();
    let mut tab = harness.memory("alice-tab", "alice").unwrap();
    let mut worker = harness.memory("alice-worker", "alice").unwrap();
    worker.create_project("project-1", "Launch notes").unwrap();
    worker
        .create_todo("todo-1", "Draft protocol", false, "project-1")
        .unwrap();

    let query = open_todos_query();
    let subscription_id = SubscriptionId::new("open-todos");
    let (mut downstream_conn, mut upstream_conn) = in_memory_connection_pair();
    let mut downstream = DownstreamSession::new(
        "tab-session",
        "alice-tab",
        tab.local_schema_fingerprint(),
        tab.local_policy_fingerprint(),
    );
    let mut upstream = UpstreamSession::new(
        "worker-session",
        "alice-worker",
        worker.local_schema_fingerprint(),
        worker.local_policy_fingerprint(),
    );

    downstream.open(&mut downstream_conn).unwrap();
    upstream.pump(&mut worker, &mut upstream_conn).unwrap();
    downstream.pump(&mut tab, &mut downstream_conn).unwrap();
    downstream
        .subscribe(
            &mut downstream_conn,
            subscription_id.clone(),
            query.clone(),
            SettlementTier::Local,
        )
        .unwrap();
    upstream.pump(&mut worker, &mut upstream_conn).unwrap();
    downstream.pump(&mut tab, &mut downstream_conn).unwrap();

    downstream
        .close(&mut downstream_conn, CloseReason::ClientClosed)
        .unwrap();
    upstream.pump(&mut worker, &mut upstream_conn).unwrap();

    assert!(!downstream.has_active_subscription(&subscription_id));
    assert!(downstream.is_closed());
    assert!(!downstream.is_settled(&subscription_id, SettlementTier::Local));
    assert_eq!(row_ids(tab.query(query).unwrap()), vec!["todo-1"]);
}

#[test]
fn connection_requires_handshake_before_subscribe() {
    let harness = Harness::new();
    let tab = harness.memory("alice-tab", "alice").unwrap();
    let query = open_todos_query();
    let subscription_id = SubscriptionId::new("open-todos");
    let (mut downstream_conn, _) = in_memory_connection_pair();
    let mut downstream = DownstreamSession::new(
        "tab-session",
        "alice-tab",
        tab.local_schema_fingerprint(),
        tab.local_policy_fingerprint(),
    );

    let err = downstream
        .subscribe(
            &mut downstream_conn,
            subscription_id,
            query,
            SettlementTier::Local,
        )
        .unwrap_err();

    assert!(err.to_string().contains("handshake is not established"));
}

#[test]
fn connection_rejects_duplicate_active_subscription_id() {
    let harness = Harness::new();
    let mut tab = harness.memory("alice-tab", "alice").unwrap();
    let mut worker = harness.memory("alice-worker", "alice").unwrap();
    let query = open_todos_query();
    let subscription_id = SubscriptionId::new("open-todos");
    let (mut downstream_conn, mut upstream_conn) = in_memory_connection_pair();
    let mut downstream = DownstreamSession::new(
        "tab-session",
        "alice-tab",
        tab.local_schema_fingerprint(),
        tab.local_policy_fingerprint(),
    );
    let mut upstream = UpstreamSession::new(
        "worker-session",
        "alice-worker",
        worker.local_schema_fingerprint(),
        worker.local_policy_fingerprint(),
    );

    downstream.open(&mut downstream_conn).unwrap();
    upstream.pump(&mut worker, &mut upstream_conn).unwrap();
    downstream.pump(&mut tab, &mut downstream_conn).unwrap();
    downstream
        .subscribe(
            &mut downstream_conn,
            subscription_id.clone(),
            query.clone(),
            SettlementTier::Local,
        )
        .unwrap();
    let err = downstream
        .subscribe(
            &mut downstream_conn,
            subscription_id,
            query,
            SettlementTier::Local,
        )
        .unwrap_err();

    assert!(err.to_string().contains("subscription is already active"));
    assert!(matches!(
        upstream_conn.receive_client_message(),
        Some(ClientMessage::Subscribe { .. })
    ));
    assert!(upstream_conn.receive_client_message().is_none());
}

#[test]
fn connection_subscription_is_not_settled_before_data_arrives() {
    let harness = Harness::new();
    let mut tab = harness.memory("alice-tab", "alice").unwrap();
    let mut worker = harness.memory("alice-worker", "alice").unwrap();
    let query = open_todos_query();
    let subscription_id = SubscriptionId::new("open-todos");
    let (mut downstream_conn, mut upstream_conn) = in_memory_connection_pair();
    let mut downstream = DownstreamSession::new(
        "tab-session",
        "alice-tab",
        tab.local_schema_fingerprint(),
        tab.local_policy_fingerprint(),
    );
    let mut upstream = UpstreamSession::new(
        "worker-session",
        "alice-worker",
        worker.local_schema_fingerprint(),
        worker.local_policy_fingerprint(),
    );

    downstream.open(&mut downstream_conn).unwrap();
    upstream.pump(&mut worker, &mut upstream_conn).unwrap();
    downstream.pump(&mut tab, &mut downstream_conn).unwrap();
    downstream
        .subscribe(
            &mut downstream_conn,
            subscription_id.clone(),
            query,
            SettlementTier::Local,
        )
        .unwrap();

    assert!(!downstream.is_settled(&subscription_id, SettlementTier::Local));
}

#[test]
fn connection_upstream_rejects_duplicate_subscribe_id() {
    let harness = Harness::new();
    let mut tab = harness.memory("alice-tab", "alice").unwrap();
    let mut worker = harness.memory("alice-worker", "alice").unwrap();
    worker.create_project("project-1", "Launch notes").unwrap();
    worker
        .create_todo("todo-1", "Draft protocol", false, "project-1")
        .unwrap();

    let query = open_todos_query();
    let subscription_id = SubscriptionId::new("open-todos");
    let (mut downstream_conn, mut upstream_conn) = in_memory_connection_pair();
    let mut downstream = DownstreamSession::new(
        "tab-session",
        "alice-tab",
        tab.local_schema_fingerprint(),
        tab.local_policy_fingerprint(),
    );
    let mut upstream = UpstreamSession::new(
        "worker-session",
        "alice-worker",
        worker.local_schema_fingerprint(),
        worker.local_policy_fingerprint(),
    );

    downstream.open(&mut downstream_conn).unwrap();
    upstream.pump(&mut worker, &mut upstream_conn).unwrap();
    downstream.pump(&mut tab, &mut downstream_conn).unwrap();
    downstream
        .subscribe(
            &mut downstream_conn,
            subscription_id.clone(),
            query.clone(),
            SettlementTier::Local,
        )
        .unwrap();
    upstream.pump(&mut worker, &mut upstream_conn).unwrap();
    downstream_conn.send_client_message(ClientMessage::Subscribe {
        subscription_id: subscription_id.clone(),
        query,
        requested_tier: SettlementTier::Local,
    });
    upstream.pump(&mut worker, &mut upstream_conn).unwrap();

    let ServerMessage::Data {
        message_id,
        subscription_id: first_data_subscription_id,
        ..
    } = downstream_conn
        .receive_server_message()
        .expect("first data frame")
    else {
        panic!("expected first data frame");
    };
    assert_eq!(message_id, MessageId(1));
    assert_eq!(first_data_subscription_id, Some(subscription_id.clone()));
    let ServerMessage::Settled {
        subscription_id: first_settled_subscription_id,
        ..
    } = downstream_conn
        .receive_server_message()
        .expect("first settled frame")
    else {
        panic!("expected first settled frame");
    };
    assert_eq!(first_settled_subscription_id, subscription_id.clone());
    let ServerMessage::Error(error) = downstream_conn
        .receive_server_message()
        .expect("duplicate subscribe error")
    else {
        panic!("expected duplicate subscribe error");
    };
    assert_eq!(error.code, "duplicate_subscription");
    assert_eq!(error.subscription_id, Some(subscription_id));
    assert!(downstream_conn.receive_server_message().is_none());
}

#[test]
fn connection_rejects_settled_before_handshake() {
    let harness = Harness::new();
    let mut tab = harness.memory("alice-tab", "alice").unwrap();
    let subscription_id = SubscriptionId::new("open-todos");
    let (mut downstream_conn, mut upstream_conn) = in_memory_connection_pair();
    let mut downstream = DownstreamSession::new(
        "tab-session",
        "alice-tab",
        tab.local_schema_fingerprint(),
        tab.local_policy_fingerprint(),
    );

    upstream_conn.send_server_message(ServerMessage::Settled {
        subscription_id: subscription_id.clone(),
        tier: SettlementTier::Local,
        cursor: ReplayCursor(1),
    });
    let err = downstream.pump(&mut tab, &mut downstream_conn).unwrap_err();

    assert!(err.to_string().contains("handshake is not established"));
    assert!(downstream.is_closed());
    assert!(!downstream.is_settled(&subscription_id, SettlementTier::Local));
}

#[test]
fn connection_ignores_unrequested_settled_tier() {
    let harness = Harness::new();
    let mut tab = harness.memory("alice-tab", "alice").unwrap();
    let mut worker = harness.memory("alice-worker", "alice").unwrap();
    worker.create_project("project-1", "Launch notes").unwrap();
    worker
        .create_todo("todo-1", "Draft protocol", false, "project-1")
        .unwrap();

    let query = open_todos_query();
    let bundle = worker.export_query(query.clone()).unwrap();
    let subscription_id = SubscriptionId::new("open-todos");
    let (mut downstream_conn, mut upstream_conn) = in_memory_connection_pair();
    let mut downstream = DownstreamSession::new(
        "tab-session",
        "alice-tab",
        tab.local_schema_fingerprint(),
        tab.local_policy_fingerprint(),
    );

    downstream.open(&mut downstream_conn).unwrap();
    upstream_conn.send_server_message(ServerMessage::Hello(ServerHello {
        protocol_version: ProtocolVersion(1),
        session_id: SessionId::new("worker-session"),
        node_id: "alice-worker".to_owned(),
        capabilities: ProtocolCapabilities::default(),
    }));
    downstream.pump(&mut tab, &mut downstream_conn).unwrap();
    downstream
        .subscribe(
            &mut downstream_conn,
            subscription_id.clone(),
            query.clone(),
            SettlementTier::Local,
        )
        .unwrap();

    upstream_conn.send_server_message(ServerMessage::Data {
        message_id: MessageId(1),
        subscription_id: Some(subscription_id.clone()),
        cursor: ReplayCursor(1),
        bundle,
    });
    downstream.pump(&mut tab, &mut downstream_conn).unwrap();
    upstream_conn.send_server_message(ServerMessage::Settled {
        subscription_id: subscription_id.clone(),
        tier: SettlementTier::Global,
        cursor: ReplayCursor(1),
    });
    downstream.pump(&mut tab, &mut downstream_conn).unwrap();

    assert_eq!(row_ids(tab.query(query).unwrap()), vec!["todo-1"]);
    assert!(!downstream.is_settled(&subscription_id, SettlementTier::Local));
    assert!(!downstream.is_settled(&subscription_id, SettlementTier::Global));
}

#[test]
fn connection_rejects_unknown_subscription_data_without_partial_apply() {
    let harness = Harness::new();
    let mut tab = harness.memory("alice-tab", "alice").unwrap();
    let mut worker = harness.memory("alice-worker", "alice").unwrap();
    worker.create_project("project-1", "Launch notes").unwrap();
    worker
        .create_todo("todo-1", "Draft protocol", false, "project-1")
        .unwrap();

    let query = open_todos_query();
    let bundle = worker.export_query(query.clone()).unwrap();
    let unknown_subscription_id = SubscriptionId::new("unknown-open-todos");
    let (mut downstream_conn, mut upstream_conn) = in_memory_connection_pair();
    let mut downstream = DownstreamSession::new(
        "tab-session",
        "alice-tab",
        tab.local_schema_fingerprint(),
        tab.local_policy_fingerprint(),
    );
    let mut upstream = UpstreamSession::new(
        "worker-session",
        "alice-worker",
        worker.local_schema_fingerprint(),
        worker.local_policy_fingerprint(),
    );

    downstream.open(&mut downstream_conn).unwrap();
    upstream.pump(&mut worker, &mut upstream_conn).unwrap();
    downstream.pump(&mut tab, &mut downstream_conn).unwrap();
    upstream_conn.send_server_message(ServerMessage::Data {
        message_id: MessageId(1),
        subscription_id: Some(unknown_subscription_id),
        cursor: ReplayCursor(1),
        bundle,
    });
    let err = downstream.pump(&mut tab, &mut downstream_conn).unwrap_err();

    assert!(err.to_string().contains("unknown subscription"));
    assert!(downstream.is_closed());
    assert!(tab.query(query).unwrap().is_empty());
    assert!(matches!(
        upstream_conn.receive_client_message(),
        Some(ClientMessage::Close(CloseReason::ProtocolError))
    ));
    assert!(upstream_conn.receive_client_message().is_none());
}

#[test]
fn connection_rejects_data_before_handshake_without_partial_apply() {
    let harness = Harness::new();
    let mut tab = harness.memory("alice-tab", "alice").unwrap();
    let mut worker = harness.memory("alice-worker", "alice").unwrap();
    worker.create_project("project-1", "Launch notes").unwrap();
    worker
        .create_todo("todo-1", "Draft protocol", false, "project-1")
        .unwrap();

    let query = open_todos_query();
    let bundle = worker.export_query(query.clone()).unwrap();
    let subscription_id = SubscriptionId::new("open-todos");
    let (mut downstream_conn, mut upstream_conn) = in_memory_connection_pair();
    let mut downstream = DownstreamSession::new(
        "tab-session",
        "alice-tab",
        tab.local_schema_fingerprint(),
        tab.local_policy_fingerprint(),
    );

    upstream_conn.send_server_message(ServerMessage::Data {
        message_id: MessageId(1),
        subscription_id: Some(subscription_id),
        cursor: ReplayCursor(1),
        bundle,
    });
    let err = downstream.pump(&mut tab, &mut downstream_conn).unwrap_err();

    assert!(err.to_string().contains("handshake is not established"));
    assert!(downstream.is_closed());
    assert_eq!(
        downstream.last_error().map(|error| error.code.as_str()),
        Some("protocol_error")
    );
    assert!(tab.query(query).unwrap().is_empty());
}

#[test]
fn connection_reconnect_requires_fresh_server_hello_before_replay() {
    let harness = Harness::new();
    let mut tab = harness.memory("alice-tab", "alice").unwrap();
    let mut worker = harness.memory("alice-worker", "alice").unwrap();
    worker.create_project("project-1", "Launch notes").unwrap();
    worker
        .create_todo("todo-1", "Draft protocol", false, "project-1")
        .unwrap();

    let query = open_todos_query();
    let subscription_id = SubscriptionId::new("open-todos");
    let (mut downstream_conn, mut upstream_conn) = in_memory_connection_pair();
    let mut downstream = DownstreamSession::new(
        "tab-session",
        "alice-tab",
        tab.local_schema_fingerprint(),
        tab.local_policy_fingerprint(),
    );
    let mut upstream = UpstreamSession::new(
        "worker-session",
        "alice-worker",
        worker.local_schema_fingerprint(),
        worker.local_policy_fingerprint(),
    );

    downstream.open(&mut downstream_conn).unwrap();
    upstream.pump(&mut worker, &mut upstream_conn).unwrap();
    downstream.pump(&mut tab, &mut downstream_conn).unwrap();
    downstream
        .subscribe(
            &mut downstream_conn,
            subscription_id,
            query,
            SettlementTier::Local,
        )
        .unwrap();

    let (mut downstream_conn, _) = in_memory_connection_pair();
    downstream.open(&mut downstream_conn).unwrap();
    let err = downstream.replay(&mut downstream_conn).unwrap_err();

    assert!(err.to_string().contains("handshake is not established"));
}

#[test]
fn connection_rejects_data_after_incompatible_server_hello_without_partial_apply() {
    let harness = Harness::new();
    let mut tab = harness.memory("alice-tab", "alice").unwrap();
    let mut worker = harness.memory("alice-worker", "alice").unwrap();
    worker.create_project("project-1", "Launch notes").unwrap();
    worker
        .create_todo("todo-1", "Draft protocol", false, "project-1")
        .unwrap();

    let query = open_todos_query();
    let bundle = worker.export_query(query.clone()).unwrap();
    let subscription_id = SubscriptionId::new("open-todos");
    let (mut downstream_conn, mut upstream_conn) = in_memory_connection_pair();
    let mut downstream = DownstreamSession::new(
        "tab-session",
        "alice-tab",
        tab.local_schema_fingerprint(),
        tab.local_policy_fingerprint(),
    );

    downstream.open(&mut downstream_conn).unwrap();
    upstream_conn.send_server_message(ServerMessage::Hello(ServerHello {
        protocol_version: ProtocolVersion(2),
        session_id: SessionId::new("worker-session"),
        node_id: "alice-worker".to_owned(),
        capabilities: ProtocolCapabilities::default(),
    }));
    upstream_conn.send_server_message(ServerMessage::Data {
        message_id: MessageId(1),
        subscription_id: Some(subscription_id),
        cursor: ReplayCursor(1),
        bundle,
    });
    downstream.pump(&mut tab, &mut downstream_conn).unwrap();

    assert!(downstream.is_closed());
    assert_eq!(
        downstream.last_error().map(|error| error.code.as_str()),
        Some("unsupported_protocol_version")
    );
    assert!(tab.query(query).unwrap().is_empty());
}

#[test]
fn connection_rejects_unsolicited_server_hello() {
    let harness = Harness::new();
    let mut tab = harness.memory("alice-tab", "alice").unwrap();
    let (mut downstream_conn, mut upstream_conn) = in_memory_connection_pair();
    let mut downstream = DownstreamSession::new(
        "tab-session",
        "alice-tab",
        tab.local_schema_fingerprint(),
        tab.local_policy_fingerprint(),
    );

    upstream_conn.send_server_message(ServerMessage::Hello(ServerHello {
        protocol_version: ProtocolVersion(1),
        session_id: SessionId::new("worker-session"),
        node_id: "alice-worker".to_owned(),
        capabilities: ProtocolCapabilities::default(),
    }));
    downstream.pump(&mut tab, &mut downstream_conn).unwrap();

    assert!(downstream.is_closed());
    assert_eq!(
        downstream.last_error().map(|error| error.code.as_str()),
        Some("protocol_error")
    );
}

#[test]
fn connection_downstream_closes_on_fatal_error_frame() {
    let harness = Harness::new();
    let mut tab = harness.memory("alice-tab", "alice").unwrap();
    let (mut downstream_conn, mut upstream_conn) = in_memory_connection_pair();
    let mut downstream = DownstreamSession::new(
        "tab-session",
        "alice-tab",
        tab.local_schema_fingerprint(),
        tab.local_policy_fingerprint(),
    );

    upstream_conn.send_server_message(ServerMessage::Error(ProtocolError::new(
        "transport_failed",
        "transport failed",
        RetryHint::Fatal,
    )));
    downstream.pump(&mut tab, &mut downstream_conn).unwrap();

    assert!(downstream.is_closed());
    assert_eq!(
        downstream.last_error().map(|error| error.code.as_str()),
        Some("transport_failed")
    );
}

#[test]
fn connection_upstream_closes_on_subscribe_before_handshake() {
    let harness = Harness::new();
    let mut worker = harness.memory("alice-worker", "alice").unwrap();
    let query = open_todos_query();
    let subscription_id = SubscriptionId::new("open-todos");
    let (mut downstream_conn, mut upstream_conn) = in_memory_connection_pair();
    let mut upstream = UpstreamSession::new(
        "worker-session",
        "alice-worker",
        worker.local_schema_fingerprint(),
        worker.local_policy_fingerprint(),
    );

    downstream_conn.send_client_message(ClientMessage::Subscribe {
        subscription_id,
        query,
        requested_tier: SettlementTier::Local,
    });
    upstream.pump(&mut worker, &mut upstream_conn).unwrap();

    assert!(upstream.is_closed());
    assert_eq!(
        upstream.last_error().map(|error| error.code.as_str()),
        Some("protocol_error")
    );
}

#[test]
fn connection_rejects_server_hello_without_ack_capability() {
    let harness = Harness::new();
    let mut tab = harness.memory("alice-tab", "alice").unwrap();
    let (mut downstream_conn, mut upstream_conn) = in_memory_connection_pair();
    let mut downstream = DownstreamSession::new(
        "tab-session",
        "alice-tab",
        tab.local_schema_fingerprint(),
        tab.local_policy_fingerprint(),
    );

    downstream.open(&mut downstream_conn).unwrap();
    upstream_conn.send_server_message(ServerMessage::Hello(ServerHello {
        protocol_version: ProtocolVersion(1),
        session_id: SessionId::new("worker-session"),
        node_id: "alice-worker".to_owned(),
        capabilities: ProtocolCapabilities {
            acknowledgements: false,
            ..ProtocolCapabilities::default()
        },
    }));
    downstream.pump(&mut tab, &mut downstream_conn).unwrap();

    assert!(downstream.is_closed());
    assert_eq!(
        downstream.last_error().map(|error| error.code.as_str()),
        Some("unsupported_capability")
    );
}

#[test]
fn connection_ignores_ack_after_close() {
    let harness = Harness::new();
    let mut tab = harness.memory("alice-tab", "alice").unwrap();
    let mut worker = harness.memory("alice-worker", "alice").unwrap();
    worker.create_project("project-1", "Launch notes").unwrap();
    worker
        .create_todo("todo-1", "Draft protocol", false, "project-1")
        .unwrap();

    let query = open_todos_query();
    let subscription_id = SubscriptionId::new("open-todos");
    let (mut downstream_conn, mut upstream_conn) = in_memory_connection_pair();
    let mut downstream = DownstreamSession::new(
        "tab-session",
        "alice-tab",
        tab.local_schema_fingerprint(),
        tab.local_policy_fingerprint(),
    );
    let mut upstream = UpstreamSession::new(
        "worker-session",
        "alice-worker",
        worker.local_schema_fingerprint(),
        worker.local_policy_fingerprint(),
    );

    downstream.open(&mut downstream_conn).unwrap();
    upstream.pump(&mut worker, &mut upstream_conn).unwrap();
    downstream.pump(&mut tab, &mut downstream_conn).unwrap();
    downstream
        .subscribe(
            &mut downstream_conn,
            subscription_id.clone(),
            query,
            SettlementTier::Local,
        )
        .unwrap();
    upstream.pump(&mut worker, &mut upstream_conn).unwrap();
    downstream
        .close(&mut downstream_conn, CloseReason::ClientClosed)
        .unwrap();
    upstream.pump(&mut worker, &mut upstream_conn).unwrap();

    downstream_conn.send_client_message(ClientMessage::Ack {
        message_id: MessageId(1),
        cursor: Some(ReplayCursor(1)),
    });
    upstream.pump(&mut worker, &mut upstream_conn).unwrap();

    assert_eq!(upstream.last_acknowledged_cursor(&subscription_id), None);
}

#[test]
fn connection_upstream_closes_on_ack_before_handshake() {
    let harness = Harness::new();
    let mut worker = harness.memory("alice-worker", "alice").unwrap();
    let subscription_id = SubscriptionId::new("open-todos");
    let (mut downstream_conn, mut upstream_conn) = in_memory_connection_pair();
    let mut upstream = UpstreamSession::new(
        "worker-session",
        "alice-worker",
        worker.local_schema_fingerprint(),
        worker.local_policy_fingerprint(),
    );

    downstream_conn.send_client_message(ClientMessage::Ack {
        message_id: MessageId(1),
        cursor: Some(ReplayCursor(1)),
    });
    upstream.pump(&mut worker, &mut upstream_conn).unwrap();

    assert!(upstream.is_closed());
    assert_eq!(
        upstream.last_error().map(|error| error.code.as_str()),
        Some("protocol_error")
    );
    assert_eq!(upstream.last_acknowledged_cursor(&subscription_id), None);
}

#[test]
fn connection_ack_tracking_never_moves_cursor_backwards() {
    let harness = Harness::new();
    let mut tab = harness.memory("alice-tab", "alice").unwrap();
    let mut worker = harness.memory("alice-worker", "alice").unwrap();
    worker.create_project("project-1", "Launch notes").unwrap();
    worker
        .create_todo("todo-1", "Draft protocol", false, "project-1")
        .unwrap();

    let query = open_todos_query();
    let subscription_id = SubscriptionId::new("open-todos");
    let (mut downstream_conn, mut upstream_conn) = in_memory_connection_pair();
    let mut downstream = DownstreamSession::new(
        "tab-session",
        "alice-tab",
        tab.local_schema_fingerprint(),
        tab.local_policy_fingerprint(),
    );
    let mut upstream = UpstreamSession::new(
        "worker-session",
        "alice-worker",
        worker.local_schema_fingerprint(),
        worker.local_policy_fingerprint(),
    );

    downstream.open(&mut downstream_conn).unwrap();
    upstream.pump(&mut worker, &mut upstream_conn).unwrap();
    downstream.pump(&mut tab, &mut downstream_conn).unwrap();
    downstream
        .subscribe(
            &mut downstream_conn,
            subscription_id.clone(),
            query,
            SettlementTier::Local,
        )
        .unwrap();
    upstream.pump(&mut worker, &mut upstream_conn).unwrap();

    downstream_conn.send_client_message(ClientMessage::Ack {
        message_id: MessageId(1),
        cursor: Some(ReplayCursor(0)),
    });
    upstream.pump(&mut worker, &mut upstream_conn).unwrap();

    assert_eq!(
        upstream.last_acknowledged_cursor(&subscription_id),
        Some(ReplayCursor(1))
    );
}

#[test]
fn connection_ack_tracking_does_not_trust_unsent_cursor() {
    let harness = Harness::new();
    let mut tab = harness.memory("alice-tab", "alice").unwrap();
    let mut worker = harness.memory("alice-worker", "alice").unwrap();
    worker.create_project("project-1", "Launch notes").unwrap();
    worker
        .create_todo("todo-1", "Draft protocol", false, "project-1")
        .unwrap();

    let query = open_todos_query();
    let subscription_id = SubscriptionId::new("open-todos");
    let (mut downstream_conn, mut upstream_conn) = in_memory_connection_pair();
    let mut downstream = DownstreamSession::new(
        "tab-session",
        "alice-tab",
        tab.local_schema_fingerprint(),
        tab.local_policy_fingerprint(),
    );
    let mut upstream = UpstreamSession::new(
        "worker-session",
        "alice-worker",
        worker.local_schema_fingerprint(),
        worker.local_policy_fingerprint(),
    );

    downstream.open(&mut downstream_conn).unwrap();
    upstream.pump(&mut worker, &mut upstream_conn).unwrap();
    downstream.pump(&mut tab, &mut downstream_conn).unwrap();
    downstream
        .subscribe(
            &mut downstream_conn,
            subscription_id.clone(),
            query,
            SettlementTier::Local,
        )
        .unwrap();
    upstream.pump(&mut worker, &mut upstream_conn).unwrap();

    downstream_conn.send_client_message(ClientMessage::Ack {
        message_id: MessageId(1),
        cursor: Some(ReplayCursor(999)),
    });
    upstream.pump(&mut worker, &mut upstream_conn).unwrap();

    assert_eq!(
        upstream.last_acknowledged_cursor(&subscription_id),
        Some(ReplayCursor(1))
    );
}

#[test]
fn connection_ack_tracking_keeps_highest_cursor_when_acks_arrive_out_of_order() {
    let harness = Harness::new();
    let mut tab = harness.memory("alice-tab", "alice").unwrap();
    let mut worker = harness.memory("alice-worker", "alice").unwrap();
    worker.create_project("project-1", "Launch notes").unwrap();
    worker
        .create_todo("todo-1", "Draft protocol", false, "project-1")
        .unwrap();

    let query = open_todos_query();
    let subscription_id = SubscriptionId::new("open-todos");
    let (mut downstream_conn, mut upstream_conn) = in_memory_connection_pair();
    let mut downstream = DownstreamSession::new(
        "tab-session",
        "alice-tab",
        tab.local_schema_fingerprint(),
        tab.local_policy_fingerprint(),
    );
    let mut upstream = UpstreamSession::new(
        "worker-session",
        "alice-worker",
        worker.local_schema_fingerprint(),
        worker.local_policy_fingerprint(),
    );

    downstream.open(&mut downstream_conn).unwrap();
    upstream.pump(&mut worker, &mut upstream_conn).unwrap();
    downstream.pump(&mut tab, &mut downstream_conn).unwrap();
    downstream
        .subscribe(
            &mut downstream_conn,
            subscription_id.clone(),
            query.clone(),
            SettlementTier::Local,
        )
        .unwrap();
    upstream.pump(&mut worker, &mut upstream_conn).unwrap();
    downstream.replay(&mut downstream_conn).unwrap();
    upstream.pump(&mut worker, &mut upstream_conn).unwrap();

    downstream_conn.send_client_message(ClientMessage::Ack {
        message_id: MessageId(2),
        cursor: Some(ReplayCursor(2)),
    });
    upstream.pump(&mut worker, &mut upstream_conn).unwrap();
    downstream_conn.send_client_message(ClientMessage::Ack {
        message_id: MessageId(1),
        cursor: Some(ReplayCursor(1)),
    });
    upstream.pump(&mut worker, &mut upstream_conn).unwrap();

    assert_eq!(
        upstream.last_acknowledged_cursor(&subscription_id),
        Some(ReplayCursor(2))
    );
}

#[test]
fn connection_downstream_replay_cursor_never_moves_backwards() {
    let harness = Harness::new();
    let mut tab = harness.memory("alice-tab", "alice").unwrap();
    let mut worker = harness.memory("alice-worker", "alice").unwrap();
    worker.create_project("project-1", "Launch notes").unwrap();
    worker
        .create_todo("todo-1", "Draft protocol", false, "project-1")
        .unwrap();

    let query = open_todos_query();
    let bundle = worker.export_query(query.clone()).unwrap();
    let subscription_id = SubscriptionId::new("open-todos");
    let (mut downstream_conn, mut upstream_conn) = in_memory_connection_pair();
    let mut downstream = DownstreamSession::new(
        "tab-session",
        "alice-tab",
        tab.local_schema_fingerprint(),
        tab.local_policy_fingerprint(),
    );

    downstream.open(&mut downstream_conn).unwrap();
    upstream_conn.send_server_message(ServerMessage::Hello(ServerHello {
        protocol_version: ProtocolVersion(1),
        session_id: SessionId::new("worker-session"),
        node_id: "alice-worker".to_owned(),
        capabilities: ProtocolCapabilities::default(),
    }));
    downstream.pump(&mut tab, &mut downstream_conn).unwrap();
    downstream
        .subscribe(
            &mut downstream_conn,
            subscription_id.clone(),
            query,
            SettlementTier::Local,
        )
        .unwrap();

    upstream_conn.send_server_message(ServerMessage::Data {
        message_id: MessageId(2),
        subscription_id: Some(subscription_id.clone()),
        cursor: ReplayCursor(2),
        bundle: bundle.clone(),
    });
    downstream.pump(&mut tab, &mut downstream_conn).unwrap();
    upstream_conn.send_server_message(ServerMessage::Data {
        message_id: MessageId(1),
        subscription_id: Some(subscription_id.clone()),
        cursor: ReplayCursor(1),
        bundle,
    });
    downstream.pump(&mut tab, &mut downstream_conn).unwrap();

    let (mut downstream_conn, mut upstream_conn) = in_memory_connection_pair();
    downstream.open(&mut downstream_conn).unwrap();
    upstream_conn.send_server_message(ServerMessage::Hello(ServerHello {
        protocol_version: ProtocolVersion(1),
        session_id: SessionId::new("worker-session-2"),
        node_id: "alice-worker".to_owned(),
        capabilities: ProtocolCapabilities::default(),
    }));
    downstream.pump(&mut tab, &mut downstream_conn).unwrap();
    let _ = upstream_conn.receive_client_message();
    downstream.replay(&mut downstream_conn).unwrap();

    let replay = upstream_conn
        .receive_client_message()
        .expect("replay message after reconnect");
    let ClientMessage::Replay { subscriptions } = replay else {
        panic!("expected replay message");
    };
    assert_eq!(subscriptions[0].last_applied_cursor, Some(ReplayCursor(2)));
}

#[test]
fn connection_upstream_new_hello_resets_session_state() {
    let harness = Harness::new();
    let mut tab = harness.memory("alice-tab", "alice").unwrap();
    let mut worker = harness.memory("alice-worker", "alice").unwrap();
    worker.create_project("project-1", "Launch notes").unwrap();
    worker
        .create_todo("todo-1", "Draft protocol", false, "project-1")
        .unwrap();

    let query = open_todos_query();
    let subscription_id = SubscriptionId::new("open-todos");
    let (mut downstream_conn, mut upstream_conn) = in_memory_connection_pair();
    let mut downstream = DownstreamSession::new(
        "tab-session",
        "alice-tab",
        tab.local_schema_fingerprint(),
        tab.local_policy_fingerprint(),
    );
    let mut upstream = UpstreamSession::new(
        "worker-session",
        "alice-worker",
        worker.local_schema_fingerprint(),
        worker.local_policy_fingerprint(),
    );

    downstream.open(&mut downstream_conn).unwrap();
    upstream.pump(&mut worker, &mut upstream_conn).unwrap();
    downstream.pump(&mut tab, &mut downstream_conn).unwrap();
    downstream
        .subscribe(
            &mut downstream_conn,
            subscription_id.clone(),
            query,
            SettlementTier::Local,
        )
        .unwrap();
    upstream.pump(&mut worker, &mut upstream_conn).unwrap();
    downstream_conn.send_client_message(ClientMessage::Ack {
        message_id: MessageId(1),
        cursor: Some(ReplayCursor(1)),
    });
    upstream.pump(&mut worker, &mut upstream_conn).unwrap();
    assert_eq!(
        upstream.last_acknowledged_cursor(&subscription_id),
        Some(ReplayCursor(1))
    );

    downstream_conn.send_client_message(ClientMessage::Hello(ClientHello {
        protocol_version: ProtocolVersion(1),
        session_id: SessionId::new("tab-session-2"),
        node_id: "alice-tab".to_owned(),
        schema_fingerprint: tab.local_schema_fingerprint(),
        policy_fingerprint: tab.local_policy_fingerprint(),
    }));
    upstream.pump(&mut worker, &mut upstream_conn).unwrap();

    assert_eq!(upstream.last_acknowledged_cursor(&subscription_id), None);
    assert_eq!(upstream.last_error(), None);
}

#[test]
fn connection_upstream_replay_prunes_omitted_subscription_state() {
    let harness = Harness::new();
    let tab = harness.memory("alice-tab", "alice").unwrap();
    let mut worker = harness.memory("alice-worker", "alice").unwrap();
    worker.create_project("project-1", "Launch notes").unwrap();
    worker
        .create_todo("todo-1", "Draft protocol", false, "project-1")
        .unwrap();

    let todos_query = open_todos_query();
    let projects_query = BuiltQuery {
        table: "projects".to_owned(),
        conditions: Vec::new(),
        order_by: Vec::new(),
        limit: None,
        offset: None,
    };
    let dropped_subscription_id = SubscriptionId::new("open-todos");
    let kept_subscription_id = SubscriptionId::new("projects");
    let (mut downstream_conn, mut upstream_conn) = in_memory_connection_pair();
    let mut upstream = UpstreamSession::new(
        "worker-session",
        "alice-worker",
        worker.local_schema_fingerprint(),
        worker.local_policy_fingerprint(),
    );

    downstream_conn.send_client_message(ClientMessage::Hello(ClientHello {
        protocol_version: ProtocolVersion(1),
        session_id: SessionId::new("tab-session"),
        node_id: "alice-tab".to_owned(),
        schema_fingerprint: tab.local_schema_fingerprint(),
        policy_fingerprint: tab.local_policy_fingerprint(),
    }));
    upstream.pump(&mut worker, &mut upstream_conn).unwrap();
    drain_server_messages(&mut downstream_conn);
    downstream_conn.send_client_message(ClientMessage::Subscribe {
        subscription_id: dropped_subscription_id.clone(),
        query: todos_query.clone(),
        requested_tier: SettlementTier::Local,
    });
    downstream_conn.send_client_message(ClientMessage::Subscribe {
        subscription_id: kept_subscription_id.clone(),
        query: projects_query.clone(),
        requested_tier: SettlementTier::Local,
    });
    upstream.pump(&mut worker, &mut upstream_conn).unwrap();
    drain_server_messages(&mut downstream_conn);

    downstream_conn.send_client_message(ClientMessage::Replay {
        subscriptions: vec![ReplaySubscription {
            subscription_id: kept_subscription_id.clone(),
            query: projects_query,
            requested_tier: SettlementTier::Local,
            last_applied_cursor: None,
        }],
    });
    upstream.pump(&mut worker, &mut upstream_conn).unwrap();
    drain_server_messages(&mut downstream_conn);
    downstream_conn.send_client_message(ClientMessage::Ack {
        message_id: MessageId(1),
        cursor: Some(ReplayCursor(1)),
    });
    upstream.pump(&mut worker, &mut upstream_conn).unwrap();
    downstream_conn.send_client_message(ClientMessage::Subscribe {
        subscription_id: dropped_subscription_id.clone(),
        query: todos_query,
        requested_tier: SettlementTier::Local,
    });
    upstream.pump(&mut worker, &mut upstream_conn).unwrap();

    assert_eq!(
        upstream.last_acknowledged_cursor(&dropped_subscription_id),
        None
    );
    let server_messages = drain_server_messages(&mut downstream_conn);
    assert!(server_messages.iter().any(|message| {
        matches!(
            message,
            ServerMessage::Data {
                subscription_id: Some(id),
                ..
            } if id == &dropped_subscription_id
        )
    }));
    assert!(!server_messages.iter().any(|message| {
        matches!(
            message,
            ServerMessage::Error(error)
                if error.subscription_id.as_ref() == Some(&dropped_subscription_id)
                    && error.code == "duplicate_subscription"
        )
    }));
}

#[test]
fn connection_rejects_incompatible_protocol_without_partial_apply() {
    let harness = Harness::new();
    let mut tab = harness.memory("alice-tab", "alice").unwrap();
    let mut worker = harness.memory("alice-worker", "alice").unwrap();
    worker.create_project("project-1", "Launch notes").unwrap();
    worker
        .create_todo("todo-1", "Draft protocol", false, "project-1")
        .unwrap();

    let query = open_todos_query();
    let (mut downstream_conn, mut upstream_conn) = in_memory_connection_pair();
    let mut downstream = DownstreamSession::new_with_protocol_version(
        "tab-session",
        "alice-tab",
        2,
        tab.local_schema_fingerprint(),
        tab.local_policy_fingerprint(),
    );
    let mut upstream = UpstreamSession::new(
        "worker-session",
        "alice-worker",
        worker.local_schema_fingerprint(),
        worker.local_policy_fingerprint(),
    );

    downstream.open(&mut downstream_conn).unwrap();
    upstream.pump(&mut worker, &mut upstream_conn).unwrap();
    assert_eq!(
        upstream.last_error().map(|error| error.code.as_str()),
        Some("unsupported_protocol_version")
    );
    downstream.pump(&mut tab, &mut downstream_conn).unwrap();

    assert!(downstream.is_closed());
    assert_eq!(
        downstream.last_error().map(|error| error.code.as_str()),
        Some("unsupported_protocol_version")
    );
    assert!(tab.query(query).unwrap().is_empty());
}

#[test]
fn connection_rejects_incompatible_schema_fingerprint_without_partial_apply() {
    let harness = Harness::new();
    let mut tab = harness.memory("alice-tab", "alice").unwrap();
    let mut worker = harness.memory("alice-worker", "alice").unwrap();
    worker.create_project("project-1", "Launch notes").unwrap();
    worker
        .create_todo("todo-1", "Draft protocol", false, "project-1")
        .unwrap();

    let query = open_todos_query();
    let (mut downstream_conn, mut upstream_conn) = in_memory_connection_pair();
    let mut downstream = DownstreamSession::new(
        "tab-session",
        "alice-tab",
        "wrong-schema",
        tab.local_policy_fingerprint(),
    );
    let mut upstream = UpstreamSession::new(
        "worker-session",
        "alice-worker",
        worker.local_schema_fingerprint(),
        worker.local_policy_fingerprint(),
    );

    downstream.open(&mut downstream_conn).unwrap();
    upstream.pump(&mut worker, &mut upstream_conn).unwrap();
    assert_eq!(
        upstream.last_error().map(|error| error.code.as_str()),
        Some("incompatible_schema_fingerprint")
    );
    downstream.pump(&mut tab, &mut downstream_conn).unwrap();

    assert!(downstream.is_closed());
    assert_eq!(
        downstream.last_error().map(|error| error.code.as_str()),
        Some("incompatible_schema_fingerprint")
    );
    assert!(tab.query(query).unwrap().is_empty());
}

#[test]
fn connection_rejects_incompatible_policy_fingerprint_without_partial_apply() {
    let harness = Harness::new();
    let mut tab = harness.memory("alice-tab", "alice").unwrap();
    let mut worker = harness.memory("alice-worker", "alice").unwrap();
    worker.create_project("project-1", "Launch notes").unwrap();
    worker
        .create_todo("todo-1", "Draft protocol", false, "project-1")
        .unwrap();

    let query = open_todos_query();
    let (mut downstream_conn, mut upstream_conn) = in_memory_connection_pair();
    let mut downstream = DownstreamSession::new(
        "tab-session",
        "alice-tab",
        tab.local_schema_fingerprint(),
        "wrong-policy",
    );
    let mut upstream = UpstreamSession::new(
        "worker-session",
        "alice-worker",
        worker.local_schema_fingerprint(),
        worker.local_policy_fingerprint(),
    );

    downstream.open(&mut downstream_conn).unwrap();
    upstream.pump(&mut worker, &mut upstream_conn).unwrap();
    assert_eq!(
        upstream.last_error().map(|error| error.code.as_str()),
        Some("incompatible_policy_fingerprint")
    );
    downstream.pump(&mut tab, &mut downstream_conn).unwrap();

    assert!(downstream.is_closed());
    assert_eq!(
        downstream.last_error().map(|error| error.code.as_str()),
        Some("incompatible_policy_fingerprint")
    );
    assert!(tab.query(query).unwrap().is_empty());
}

#[test]
fn connection_subscribe_rejection_is_protocol_error_frame() {
    let harness = Harness::new();
    let mut tab = harness.memory("alice-tab", "alice").unwrap();
    let mut worker = harness.memory("alice-worker", "alice").unwrap();
    let bad_query = BuiltQuery {
        table: "missing_table".to_owned(),
        conditions: Vec::new(),
        order_by: Vec::new(),
        limit: None,
        offset: None,
    };
    let subscription_id = SubscriptionId::new("bad-query");
    let (mut downstream_conn, mut upstream_conn) = in_memory_connection_pair();
    let mut downstream = DownstreamSession::new(
        "tab-session",
        "alice-tab",
        tab.local_schema_fingerprint(),
        tab.local_policy_fingerprint(),
    );
    let mut upstream = UpstreamSession::new(
        "worker-session",
        "alice-worker",
        worker.local_schema_fingerprint(),
        worker.local_policy_fingerprint(),
    );

    downstream.open(&mut downstream_conn).unwrap();
    upstream.pump(&mut worker, &mut upstream_conn).unwrap();
    downstream.pump(&mut tab, &mut downstream_conn).unwrap();
    downstream
        .subscribe(
            &mut downstream_conn,
            subscription_id.clone(),
            bad_query,
            SettlementTier::Local,
        )
        .unwrap();
    upstream.pump(&mut worker, &mut upstream_conn).unwrap();
    downstream.pump(&mut tab, &mut downstream_conn).unwrap();

    let error = downstream.last_error().expect("protocol error frame");
    assert_eq!(error.code, "query_rejected");
    assert_eq!(error.subscription_id, Some(subscription_id.clone()));
    assert!(!downstream.is_closed());
    assert!(!downstream.has_active_subscription(&subscription_id));
}

#[test]
fn connection_closes_when_data_bundle_apply_fails() {
    let harness = Harness::new();
    let mut tab = harness.memory("alice-tab", "alice").unwrap();
    let mut worker = harness.memory("alice-worker", "alice").unwrap();
    worker.create_project("project-1", "Launch notes").unwrap();
    worker
        .create_todo("todo-1", "Draft protocol", false, "project-1")
        .unwrap();

    let query = open_todos_query();
    let mut bundle = worker.export_query(query.clone()).unwrap();
    bundle.protocol_version = mini_jazz_sqlite::sync::BUNDLE_PROTOCOL_VERSION + 1;
    let (mut downstream_conn, mut upstream_conn) = in_memory_connection_pair();
    let mut downstream = DownstreamSession::new(
        "tab-session",
        "alice-tab",
        tab.local_schema_fingerprint(),
        tab.local_policy_fingerprint(),
    );

    downstream.open(&mut downstream_conn).unwrap();
    upstream_conn.send_server_message(ServerMessage::Hello(ServerHello {
        protocol_version: ProtocolVersion(1),
        session_id: SessionId::new("worker-session"),
        node_id: "alice-worker".to_owned(),
        capabilities: ProtocolCapabilities::default(),
    }));
    upstream_conn.send_server_message(ServerMessage::Data {
        message_id: MessageId(1),
        subscription_id: None,
        cursor: ReplayCursor(1),
        bundle,
    });
    let err = downstream.pump(&mut tab, &mut downstream_conn).unwrap_err();

    assert!(err
        .to_string()
        .contains("unsupported bundle protocol version"));
    assert!(downstream.is_closed());
    assert_eq!(
        downstream.last_error().map(|error| error.code.as_str()),
        Some("bundle_apply_failed")
    );
    assert!(tab.query(query).unwrap().is_empty());
}

#[test]
fn connection_can_run_tab_to_worker_and_worker_to_authority_with_same_messages() {
    let harness = Harness::new();
    let mut tab = harness.memory("alice-tab", "alice").unwrap();
    let mut worker = harness.memory("alice-worker", "alice").unwrap();
    let mut authority = harness.memory("authority", "alice").unwrap();
    authority
        .create_project("project-1", "Launch notes")
        .unwrap();
    authority
        .create_todo("todo-1", "Draft protocol", false, "project-1")
        .unwrap();

    let query = open_todos_query();
    let worker_subscription = SubscriptionId::new("worker-open-todos");
    let (mut worker_downstream_conn, mut authority_upstream_conn) = in_memory_connection_pair();
    let mut worker_downstream = DownstreamSession::new(
        "worker-downstream-session",
        "alice-worker",
        worker.local_schema_fingerprint(),
        worker.local_policy_fingerprint(),
    );
    let mut authority_upstream = UpstreamSession::new(
        "authority-session",
        "authority",
        authority.local_schema_fingerprint(),
        authority.local_policy_fingerprint(),
    );

    worker_downstream.open(&mut worker_downstream_conn).unwrap();
    authority_upstream
        .pump(&mut authority, &mut authority_upstream_conn)
        .unwrap();
    worker_downstream
        .pump(&mut worker, &mut worker_downstream_conn)
        .unwrap();
    worker_downstream
        .subscribe(
            &mut worker_downstream_conn,
            worker_subscription,
            query.clone(),
            SettlementTier::Local,
        )
        .unwrap();
    authority_upstream
        .pump(&mut authority, &mut authority_upstream_conn)
        .unwrap();
    worker_downstream
        .pump(&mut worker, &mut worker_downstream_conn)
        .unwrap();
    assert_eq!(
        row_ids(worker.query(query.clone()).unwrap()),
        vec!["todo-1"]
    );

    let tab_subscription = SubscriptionId::new("tab-open-todos");
    let (mut tab_downstream_conn, mut worker_upstream_conn) = in_memory_connection_pair();
    let mut tab_downstream = DownstreamSession::new(
        "tab-session",
        "alice-tab",
        tab.local_schema_fingerprint(),
        tab.local_policy_fingerprint(),
    );
    let mut worker_upstream = UpstreamSession::new(
        "worker-upstream-session",
        "alice-worker",
        worker.local_schema_fingerprint(),
        worker.local_policy_fingerprint(),
    );

    tab_downstream.open(&mut tab_downstream_conn).unwrap();
    worker_upstream
        .pump(&mut worker, &mut worker_upstream_conn)
        .unwrap();
    tab_downstream
        .pump(&mut tab, &mut tab_downstream_conn)
        .unwrap();
    tab_downstream
        .subscribe(
            &mut tab_downstream_conn,
            tab_subscription,
            query.clone(),
            SettlementTier::Local,
        )
        .unwrap();
    worker_upstream
        .pump(&mut worker, &mut worker_upstream_conn)
        .unwrap();
    tab_downstream
        .pump(&mut tab, &mut tab_downstream_conn)
        .unwrap();

    assert_eq!(row_ids(tab.query(query).unwrap()), vec!["todo-1"]);
}

#[test]
fn connection_manager_open_subscribe_applies_data_and_emits_ack() {
    let harness = Harness::new();
    let mut tab = harness.memory("alice-tab", "alice").unwrap();
    let mut worker = harness.memory("alice-worker", "alice").unwrap();
    worker.create_project("project-1", "Launch notes").unwrap();
    worker
        .create_todo("todo-1", "Connection manager protocol", false, "project-1")
        .unwrap();

    let query = open_todos_query();
    let mut downstream = DownstreamConnectionManager::new(
        "tab-session",
        "alice-tab",
        tab.local_schema_fingerprint(),
        tab.local_policy_fingerprint(),
    );
    let mut upstream = UpstreamConnectionManager::new(
        "worker-session",
        "alice-worker",
        worker.local_schema_fingerprint(),
        worker.local_policy_fingerprint(),
    );

    let server_messages = upstream
        .receive(&mut worker, downstream.open().unwrap())
        .unwrap();
    assert!(!downstream.is_ready());
    let client_messages = downstream.receive(&mut tab, server_messages).unwrap();
    assert!(client_messages.is_empty());
    assert!(downstream.is_ready());

    let (subscription, client_messages) = downstream
        .subscribe(query.clone(), SettlementTier::Local)
        .unwrap();
    let server_messages = upstream.receive(&mut worker, client_messages).unwrap();
    let client_messages = downstream.receive(&mut tab, server_messages).unwrap();

    assert!(downstream.is_settled(&subscription, SettlementTier::Local));
    assert!(matches!(
        client_messages.as_slice(),
        [ClientMessage::Ack {
            message_id: MessageId(1),
            cursor: Some(ReplayCursor(1))
        }]
    ));
    upstream.receive(&mut worker, client_messages).unwrap();
    assert_eq!(row_ids(tab.query(query).unwrap()), vec!["todo-1"]);
}

#[test]
fn connection_manager_queues_subscribe_until_handshake_is_ready() {
    let harness = Harness::new();
    let mut tab = harness.memory("alice-tab", "alice").unwrap();
    let mut worker = harness.memory("alice-worker", "alice").unwrap();
    let mut downstream = DownstreamConnectionManager::new(
        "tab-session",
        "alice-tab",
        tab.local_schema_fingerprint(),
        tab.local_policy_fingerprint(),
    );
    let mut upstream = UpstreamConnectionManager::new(
        "worker-session",
        "alice-worker",
        worker.local_schema_fingerprint(),
        worker.local_policy_fingerprint(),
    );

    let open_messages = downstream.open().unwrap();
    let (subscription, queued_messages) = downstream
        .subscribe(open_todos_query(), SettlementTier::Local)
        .unwrap();
    assert!(queued_messages.is_empty());
    assert!(!downstream.is_ready());

    let server_messages = upstream.receive(&mut worker, open_messages).unwrap();
    let client_messages = downstream.receive(&mut tab, server_messages).unwrap();

    let [ClientMessage::Subscribe {
        subscription_id,
        requested_tier,
        ..
    }] = client_messages.as_slice()
    else {
        panic!("expected queued subscribe to flush after handshake");
    };
    assert_eq!(subscription_id, subscription.id());
    assert_eq!(*requested_tier, SettlementTier::Local);
    assert!(downstream.is_ready());
}

#[test]
fn connection_manager_ignores_late_frames_for_locally_dropped_subscription() {
    let harness = Harness::new();
    let mut tab = harness.memory("alice-tab", "alice").unwrap();
    let mut worker = harness.memory("alice-worker", "alice").unwrap();
    worker.create_project("project-1", "Launch notes").unwrap();
    worker
        .create_todo("todo-1", "Late frame", false, "project-1")
        .unwrap();

    let query = open_todos_query();
    let mut downstream = DownstreamConnectionManager::new(
        "tab-session",
        "alice-tab",
        tab.local_schema_fingerprint(),
        tab.local_policy_fingerprint(),
    );
    let mut upstream = UpstreamConnectionManager::new(
        "worker-session",
        "alice-worker",
        worker.local_schema_fingerprint(),
        worker.local_policy_fingerprint(),
    );

    let server_messages = upstream
        .receive(&mut worker, downstream.open().unwrap())
        .unwrap();
    downstream.receive(&mut tab, server_messages).unwrap();
    let (subscription, client_messages) = downstream
        .subscribe(query.clone(), SettlementTier::Local)
        .unwrap();
    let server_messages = upstream.receive(&mut worker, client_messages).unwrap();

    let unsubscribe_messages = downstream.unsubscribe(&subscription).unwrap();
    let client_messages = downstream.receive(&mut tab, server_messages).unwrap();

    assert!(unsubscribe_messages.iter().any(|message| {
        matches!(
            message,
            ClientMessage::Replay { subscriptions } if subscriptions.is_empty()
        )
    }));
    assert!(client_messages.is_empty());
    assert!(!downstream.is_closed());
    assert!(tab.query(query).unwrap().is_empty());
}

#[test]
fn connection_manager_unsubscribe_replays_remaining_interest_upstream() {
    let harness = Harness::new();
    let mut tab = harness.memory("alice-tab", "alice").unwrap();
    let mut worker = harness.memory("alice-worker", "alice").unwrap();
    let mut downstream = DownstreamConnectionManager::new(
        "tab-session",
        "alice-tab",
        tab.local_schema_fingerprint(),
        tab.local_policy_fingerprint(),
    );
    let mut upstream = UpstreamConnectionManager::new(
        "worker-session",
        "alice-worker",
        worker.local_schema_fingerprint(),
        worker.local_policy_fingerprint(),
    );

    let server_messages = upstream
        .receive(&mut worker, downstream.open().unwrap())
        .unwrap();
    downstream.receive(&mut tab, server_messages).unwrap();
    let (dropped_subscription, dropped_messages) = downstream
        .subscribe(open_todos_query(), SettlementTier::Local)
        .unwrap();
    let (kept_subscription, kept_messages) = downstream
        .subscribe(
            BuiltQuery {
                table: "projects".to_owned(),
                conditions: Vec::new(),
                order_by: Vec::new(),
                limit: None,
                offset: None,
            },
            SettlementTier::Local,
        )
        .unwrap();
    upstream.receive(&mut worker, dropped_messages).unwrap();
    upstream.receive(&mut worker, kept_messages).unwrap();

    let replay_messages = downstream.unsubscribe(&dropped_subscription).unwrap();

    let [ClientMessage::Replay { subscriptions }] = replay_messages.as_slice() else {
        panic!("expected one replay message");
    };
    assert_eq!(subscriptions.len(), 1);
    assert_eq!(subscriptions[0].subscription_id, *kept_subscription.id());

    upstream.receive(&mut worker, replay_messages).unwrap();

    assert!(!upstream.has_active_subscription(dropped_subscription.id()));
    assert!(upstream.has_active_subscription(kept_subscription.id()));
}

#[test]
fn connection_manager_upstream_reports_scoped_query_errors() {
    let harness = Harness::new();
    let mut tab = harness.memory("alice-tab", "alice").unwrap();
    let mut worker = harness.memory("alice-worker", "alice").unwrap();
    let mut downstream = DownstreamConnectionManager::new(
        "tab-session",
        "alice-tab",
        tab.local_schema_fingerprint(),
        tab.local_policy_fingerprint(),
    );
    let mut upstream = UpstreamConnectionManager::new(
        "worker-session",
        "alice-worker",
        worker.local_schema_fingerprint(),
        worker.local_policy_fingerprint(),
    );
    let bad_query = BuiltQuery {
        table: "missing_table".to_owned(),
        conditions: Vec::new(),
        order_by: Vec::new(),
        limit: None,
        offset: None,
    };

    let server_messages = upstream
        .receive(&mut worker, downstream.open().unwrap())
        .unwrap();
    downstream.receive(&mut tab, server_messages).unwrap();
    let (subscription, client_messages) = downstream
        .subscribe(bad_query, SettlementTier::Local)
        .unwrap();
    let server_messages = upstream.receive(&mut worker, client_messages).unwrap();

    let [ServerMessage::Error(error)] = server_messages.as_slice() else {
        panic!("expected one scoped error");
    };
    assert_eq!(error.code, "query_rejected");
    assert_eq!(error.subscription_id.as_ref(), Some(subscription.id()));
}

#[test]
fn connection_manager_downstream_surfaces_scoped_query_errors() {
    let harness = Harness::new();
    let mut tab = harness.memory("alice-tab", "alice").unwrap();
    let mut worker = harness.memory("alice-worker", "alice").unwrap();
    let mut downstream = DownstreamConnectionManager::new(
        "tab-session",
        "alice-tab",
        tab.local_schema_fingerprint(),
        tab.local_policy_fingerprint(),
    );
    let mut upstream = UpstreamConnectionManager::new(
        "worker-session",
        "alice-worker",
        worker.local_schema_fingerprint(),
        worker.local_policy_fingerprint(),
    );
    let bad_query = BuiltQuery {
        table: "missing_table".to_owned(),
        conditions: Vec::new(),
        order_by: Vec::new(),
        limit: None,
        offset: None,
    };

    let server_messages = upstream
        .receive(&mut worker, downstream.open().unwrap())
        .unwrap();
    downstream.receive(&mut tab, server_messages).unwrap();
    let (_, client_messages) = downstream
        .subscribe(bad_query, SettlementTier::Local)
        .unwrap();
    let server_messages = upstream.receive(&mut worker, client_messages).unwrap();
    let err = downstream.receive(&mut tab, server_messages).unwrap_err();

    assert!(err.to_string().contains("query_rejected"));
}

fn open_todos_query() -> BuiltQuery {
    BuiltQuery {
        table: "todos".to_owned(),
        conditions: vec![QueryCondition {
            column: "done".to_owned(),
            op: QueryConditionOp::Eq,
            value: json!(false),
        }],
        order_by: Vec::new(),
        limit: None,
        offset: None,
    }
}

fn row_ids(rows: Vec<mini_jazz_sqlite::RowView>) -> Vec<String> {
    rows.into_iter().map(|row| row.id).collect()
}

fn sorted_row_ids(rows: Vec<mini_jazz_sqlite::RowView>) -> Vec<String> {
    let mut ids = row_ids(rows);
    ids.sort();
    ids
}

fn drain_server_messages(
    conn: &mut mini_jazz_sqlite::connection::DownstreamConnection,
) -> Vec<ServerMessage> {
    let mut messages = Vec::new();
    while let Some(message) = conn.receive_server_message() {
        messages.push(message);
    }
    messages
}

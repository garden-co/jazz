use super::support::{FixtureRuntimeExt, Harness};
use mini_jazz_sqlite::connection::{
    in_memory_connection_pair, DownstreamEndpoint, UpstreamEndpoint,
};
use mini_jazz_sqlite::connection::{DownstreamConnectionManager, UpstreamConnectionManager};
use mini_jazz_sqlite::protocol::{
    ClientDataRecord, ClientHello, ClientMessage, ClientTx, CloseReason, DataOp, MessageId,
    ProtocolCapabilities, ProtocolError, ProtocolVersion, ReplayCursor, ReplaySubscription,
    RetryHint, ServerHello, ServerMessage, SessionId, SettlementTier, SubscriptionId,
    TxConflictMode, TxStatusKind,
};
use mini_jazz_sqlite::session::{DownstreamSession, UpstreamSession};
use mini_jazz_sqlite::sync::ReadRecord;
use mini_jazz_sqlite::{BuiltQuery, QueryCondition, QueryConditionOp};
use serde_json::json;
use std::collections::BTreeMap;

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
        protocol_version: ProtocolVersion(2),
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
        protocol_version: ProtocolVersion(3),
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
        protocol_version: ProtocolVersion(2),
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
        protocol_version: ProtocolVersion(2),
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
fn downstream_rejects_server_without_tx_upload_capability() {
    let harness = Harness::new();
    let mut tab = harness.memory("alice-tab", "alice").unwrap();
    let mut downstream = DownstreamConnectionManager::new(
        "tab-session",
        "alice-tab",
        tab.local_schema_fingerprint(),
        tab.local_policy_fingerprint(),
    );

    let open_messages = downstream.open().unwrap();
    assert!(matches!(
        open_messages.as_slice(),
        [ClientMessage::Hello(_)]
    ));

    let server_messages = vec![ServerMessage::Hello(ServerHello {
        protocol_version: ProtocolVersion(2),
        session_id: SessionId::new("server-session"),
        node_id: "edge".to_owned(),
        capabilities: ProtocolCapabilities {
            tx_upload: false,
            ..ProtocolCapabilities::default()
        },
    })];

    let result = downstream.receive(&mut tab, server_messages);

    assert!(result.is_ok());
    assert!(downstream.is_closed());
    assert_eq!(
        downstream.last_error().map(|error| error.code.as_str()),
        Some("unsupported_capability")
    );
}

#[test]
fn protocol_defaults_include_tx_upload_capability() {
    assert!(ProtocolCapabilities::default().tx_upload);
}

#[test]
fn connection_upstream_requires_auth_for_upload_tx_after_handshake() {
    let harness = Harness::new();
    let mut worker = harness.memory("alice-worker", "alice").unwrap();
    let (mut downstream_conn, mut upstream_conn) = in_memory_connection_pair();
    let mut upstream = UpstreamSession::new(
        "worker-session",
        "alice-worker",
        worker.local_schema_fingerprint(),
        worker.local_policy_fingerprint(),
    );

    downstream_conn.send_client_message(ClientMessage::Hello(ClientHello {
        protocol_version: ProtocolVersion(2),
        session_id: SessionId::new("tab-session"),
        node_id: "alice-tab".to_owned(),
        schema_fingerprint: worker.local_schema_fingerprint(),
        policy_fingerprint: worker.local_policy_fingerprint(),
    }));
    upstream.pump(&mut worker, &mut upstream_conn).unwrap();
    assert!(matches!(
        downstream_conn.receive_server_message(),
        Some(ServerMessage::Hello(_))
    ));

    downstream_conn.send_client_message(ClientMessage::UploadTx {
        tx: ClientTx {
            tx_id: "tx-1".to_owned(),
            branch_id: None,
            conflict_mode: TxConflictMode::Mergeable,
            created_at: 1,
            author: Some("alice".to_owned()),
        },
        data: Vec::new(),
        reads: Vec::new(),
    });
    upstream.pump(&mut worker, &mut upstream_conn).unwrap();

    assert!(upstream.is_closed());
    assert_eq!(
        upstream.last_error().map(|error| error.code.as_str()),
        Some("auth_required")
    );
    assert!(matches!(
        drain_server_messages(&mut downstream_conn).as_slice(),
        [
            ServerMessage::Error(error),
            ServerMessage::Close(CloseReason::ProtocolError)
        ] if error.code == "auth_required"
    ));
}

#[test]
fn connection_upstream_unsubscribe_removes_subscription_after_handshake() {
    let harness = Harness::new();
    let mut worker = harness.memory("alice-worker", "alice").unwrap();
    let (mut downstream_conn, mut upstream_conn) = in_memory_connection_pair();
    let mut upstream = UpstreamSession::new(
        "worker-session",
        "alice-worker",
        worker.local_schema_fingerprint(),
        worker.local_policy_fingerprint(),
    );

    downstream_conn.send_client_message(ClientMessage::Hello(ClientHello {
        protocol_version: ProtocolVersion(2),
        session_id: SessionId::new("tab-session"),
        node_id: "alice-tab".to_owned(),
        schema_fingerprint: worker.local_schema_fingerprint(),
        policy_fingerprint: worker.local_policy_fingerprint(),
    }));
    upstream.pump(&mut worker, &mut upstream_conn).unwrap();
    assert!(matches!(
        downstream_conn.receive_server_message(),
        Some(ServerMessage::Hello(_))
    ));

    let subscription_id = SubscriptionId::new("open-todos");
    downstream_conn.send_client_message(ClientMessage::Subscribe {
        subscription_id: subscription_id.clone(),
        query: open_todos_query(),
        requested_tier: SettlementTier::Local,
    });
    upstream.pump(&mut worker, &mut upstream_conn).unwrap();
    assert!(upstream.has_active_subscription(&subscription_id));
    drain_server_messages(&mut downstream_conn);

    downstream_conn.send_client_message(ClientMessage::Unsubscribe {
        subscription_id: subscription_id.clone(),
    });
    upstream.pump(&mut worker, &mut upstream_conn).unwrap();

    assert!(!upstream.is_closed());
    assert!(!upstream.has_active_subscription(&subscription_id));
    assert!(drain_server_messages(&mut downstream_conn).is_empty());
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
        protocol_version: ProtocolVersion(2),
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
        protocol_version: ProtocolVersion(2),
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
        protocol_version: ProtocolVersion(2),
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
        protocol_version: ProtocolVersion(2),
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
        1,
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
        protocol_version: ProtocolVersion(2),
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
fn connection_manager_flushes_uploads_after_handshake() {
    let harness = Harness::new();
    let mut tab = harness.memory("alice-tab", "alice").unwrap();
    let mut worker = harness.memory("alice-worker", "alice").unwrap();
    tab.create_project("project-1", "Upload plan").unwrap();
    let tx_id = tab
        .create_todo("todo-upload-loop", "Flush after hello", false, "project-1")
        .unwrap();

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
    let client_messages = downstream.receive(&mut tab, server_messages).unwrap();

    assert!(client_messages.iter().any(|message| {
        matches!(message, ClientMessage::UploadTx { tx, .. } if tx.tx_id == tx_id)
    }));
}

#[test]
fn connection_manager_flushes_new_local_upload_on_quiet_connection() {
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
    assert!(downstream
        .receive(&mut tab, server_messages)
        .unwrap()
        .is_empty());

    let tx_id = tab
        .create_project("project-quiet-upload", "Quiet upload")
        .unwrap();
    let client_messages = downstream.flush(&mut tab).unwrap();

    assert!(client_messages.iter().any(|message| {
        matches!(message, ClientMessage::UploadTx { tx, .. } if tx.tx_id == tx_id)
    }));
}

#[test]
fn upstream_upload_tx_gets_ack_and_edge_status() {
    let harness = Harness::new();
    let mut tab = harness.memory("alice-tab", "alice").unwrap();
    let mut worker = harness.memory("alice-worker", "alice").unwrap();
    tab.create_project("project-1", "Upload plan").unwrap();
    let tx_id = tab
        .create_todo("todo-upload-upstream", "Apply upload", false, "project-1")
        .unwrap();

    let mut downstream = DownstreamConnectionManager::new(
        "tab-session",
        "alice-tab",
        tab.local_schema_fingerprint(),
        tab.local_policy_fingerprint(),
    );
    let mut upstream = UpstreamConnectionManager::new_authenticated_for_test(
        "worker-session",
        "alice-worker",
        worker.local_schema_fingerprint(),
        worker.local_policy_fingerprint(),
        "alice",
    );

    let server_messages = upstream
        .receive(&mut worker, downstream.open().unwrap())
        .unwrap();
    let upload_messages = downstream.receive(&mut tab, server_messages).unwrap();
    assert!(upload_tx_ids(&upload_messages).contains(&tx_id));

    let server_messages = upstream.receive(&mut worker, upload_messages).unwrap();

    assert!(server_messages.iter().any(|message| {
        matches!(message, ServerMessage::UploadAck { tx_id: acked } if acked == &tx_id)
    }));
    assert!(server_messages.iter().any(|message| {
        matches!(
            message,
            ServerMessage::TxStatus {
                tx_id: status_tx_id,
                status: TxStatusKind::EdgeAccepted
            } if status_tx_id == &tx_id
        )
    }));
    assert_eq!(
        row_ids(worker.read_rows("todos").unwrap()),
        vec!["todo-upload-upstream"]
    );

    downstream.receive(&mut tab, server_messages).unwrap();
    assert!(!tab
        .active_uploads_for_test(10)
        .unwrap()
        .iter()
        .any(|upload| upload.tx.tx_id == tx_id));
}

#[test]
fn upstream_upload_tx_accepts_opaque_tx_id() {
    let harness = Harness::new();
    let mut worker = harness.memory("alice-worker", "alice").unwrap();
    let tx_id = "018f56e2-6e2b-7d4d-9f66-4a59421e8a8f".to_owned();

    let mut downstream = DownstreamConnectionManager::new(
        "tab-session",
        "alice-tab",
        worker.local_schema_fingerprint(),
        worker.local_policy_fingerprint(),
    );
    let mut upstream = UpstreamConnectionManager::new_authenticated_for_test(
        "worker-session",
        "alice-worker",
        worker.local_schema_fingerprint(),
        worker.local_policy_fingerprint(),
        "alice",
    );

    let server_messages = upstream
        .receive(&mut worker, downstream.open().unwrap())
        .unwrap();
    downstream.receive(&mut worker, server_messages).unwrap();

    let server_messages = upstream
        .receive(
            &mut worker,
            vec![ClientMessage::UploadTx {
                tx: ClientTx {
                    tx_id: tx_id.clone(),
                    branch_id: None,
                    conflict_mode: TxConflictMode::Mergeable,
                    created_at: 1,
                    author: Some("alice".to_owned()),
                },
                data: vec![ClientDataRecord {
                    table: "projects".to_owned(),
                    row_id: "project-opaque-upload".to_owned(),
                    op: DataOp::Insert,
                    values: BTreeMap::from([("title".to_owned(), json!("Opaque upload"))]),
                }],
                reads: Vec::new(),
            }],
        )
        .unwrap();

    assert!(server_messages.iter().any(|message| {
        matches!(message, ServerMessage::UploadAck { tx_id: acked } if acked == &tx_id)
    }));
    assert!(server_messages.iter().any(|message| {
        matches!(
            message,
            ServerMessage::TxStatus {
                tx_id: status_tx_id,
                status: TxStatusKind::EdgeAccepted
            } if status_tx_id == &tx_id
        )
    }));
    assert_eq!(
        row_ids(worker.read_rows("projects").unwrap()),
        vec!["project-opaque-upload"]
    );
}

#[test]
fn upstream_upload_update_waits_when_row_is_missing_then_applies_after_sync() {
    let harness = Harness::new();
    let mut authority = harness.memory("authority", "alice").unwrap();
    let mut worker = harness.memory("alice-worker", "alice").unwrap();
    authority
        .create_project("project-1", "Upload plan")
        .unwrap();
    authority
        .create_todo("todo-missing-update", "Original", false, "project-1")
        .unwrap();

    let mut downstream = DownstreamConnectionManager::new(
        "tab-session",
        "alice-tab",
        worker.local_schema_fingerprint(),
        worker.local_policy_fingerprint(),
    );
    let mut upstream = UpstreamConnectionManager::new_authenticated_for_test(
        "worker-session",
        "alice-worker",
        worker.local_schema_fingerprint(),
        worker.local_policy_fingerprint(),
        "alice",
    );

    let server_messages = upstream
        .receive(&mut worker, downstream.open().unwrap())
        .unwrap();
    downstream.receive(&mut worker, server_messages).unwrap();

    let tx_id = "tx-alice-tab-9101".to_owned();
    let upload = ClientMessage::UploadTx {
        tx: ClientTx {
            tx_id: tx_id.clone(),
            branch_id: None,
            conflict_mode: TxConflictMode::Mergeable,
            created_at: 1,
            author: Some("alice".to_owned()),
        },
        data: vec![ClientDataRecord {
            table: "todos".to_owned(),
            row_id: "todo-missing-update".to_owned(),
            op: DataOp::Update,
            values: BTreeMap::from([("title".to_owned(), json!("Updated after sync"))]),
        }],
        reads: Vec::new(),
    };

    let server_messages = upstream.receive(&mut worker, vec![upload.clone()]).unwrap();

    assert!(server_messages.iter().any(
        |message| matches!(message, ServerMessage::UploadAck { tx_id: acked } if acked == &tx_id)
    ));
    assert!(!server_messages.iter().any(|message| {
        matches!(
            message,
            ServerMessage::TxStatus {
                tx_id: status_tx_id,
                status: TxStatusKind::Rejected { .. }
            } if status_tx_id == &tx_id
        )
    }));
    assert!(worker.transaction_info(&tx_id).is_err());

    worker
        .apply_bundle(&authority.export_table_history("projects").unwrap())
        .unwrap();
    worker
        .apply_bundle(&authority.export_table_history("todos").unwrap())
        .unwrap();

    let server_messages = upstream.receive(&mut worker, vec![upload]).unwrap();

    assert!(server_messages.iter().any(|message| {
        matches!(
            message,
            ServerMessage::TxStatus {
                tx_id: status_tx_id,
                status: TxStatusKind::EdgeAccepted
            } if status_tx_id == &tx_id
        )
    }));
    let rows = worker.read_rows("todos").unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].id, "todo-missing-update");
    assert_eq!(rows[0].values["title"], json!("Updated after sync"));
}

#[test]
fn upstream_upload_delete_waits_when_row_is_missing_then_applies_after_sync() {
    let harness = Harness::new();
    let mut authority = harness.memory("authority", "alice").unwrap();
    let mut worker = harness.memory("alice-worker", "alice").unwrap();
    authority
        .create_project("project-1", "Upload plan")
        .unwrap();
    authority
        .create_todo(
            "todo-missing-delete",
            "Delete after sync",
            false,
            "project-1",
        )
        .unwrap();

    let mut downstream = DownstreamConnectionManager::new(
        "tab-session",
        "alice-tab",
        worker.local_schema_fingerprint(),
        worker.local_policy_fingerprint(),
    );
    let mut upstream = UpstreamConnectionManager::new_authenticated_for_test(
        "worker-session",
        "alice-worker",
        worker.local_schema_fingerprint(),
        worker.local_policy_fingerprint(),
        "alice",
    );

    let server_messages = upstream
        .receive(&mut worker, downstream.open().unwrap())
        .unwrap();
    downstream.receive(&mut worker, server_messages).unwrap();

    let tx_id = "tx-alice-tab-9102".to_owned();
    let upload = ClientMessage::UploadTx {
        tx: ClientTx {
            tx_id: tx_id.clone(),
            branch_id: None,
            conflict_mode: TxConflictMode::Mergeable,
            created_at: 1,
            author: Some("alice".to_owned()),
        },
        data: vec![ClientDataRecord {
            table: "todos".to_owned(),
            row_id: "todo-missing-delete".to_owned(),
            op: DataOp::Delete,
            values: BTreeMap::new(),
        }],
        reads: Vec::new(),
    };

    let server_messages = upstream.receive(&mut worker, vec![upload.clone()]).unwrap();

    assert!(server_messages.iter().any(
        |message| matches!(message, ServerMessage::UploadAck { tx_id: acked } if acked == &tx_id)
    ));
    assert!(!server_messages.iter().any(|message| {
        matches!(
            message,
            ServerMessage::TxStatus {
                tx_id: status_tx_id,
                status: TxStatusKind::Rejected { .. }
            } if status_tx_id == &tx_id
        )
    }));
    assert!(worker.transaction_info(&tx_id).is_err());

    worker
        .apply_bundle(&authority.export_table_history("projects").unwrap())
        .unwrap();
    worker
        .apply_bundle(&authority.export_table_history("todos").unwrap())
        .unwrap();

    let server_messages = upstream.receive(&mut worker, vec![upload]).unwrap();

    assert!(server_messages.iter().any(|message| {
        matches!(
            message,
            ServerMessage::TxStatus {
                tx_id: status_tx_id,
                status: TxStatusKind::EdgeAccepted
            } if status_tx_id == &tx_id
        )
    }));
    assert!(worker.read_rows("todos").unwrap().is_empty());
}

#[test]
fn upstream_upload_tx_dedupes_existing_tx_id_without_rewrite() {
    let harness = Harness::new();
    let mut worker = harness.memory("alice-worker", "alice").unwrap();
    let existing_tx = worker
        .create_project("project-existing", "Already here")
        .unwrap();
    worker.accept_transaction_at_edge(&existing_tx).unwrap();
    let mut downstream = DownstreamConnectionManager::new(
        "tab-session",
        "alice-tab",
        worker.local_schema_fingerprint(),
        worker.local_policy_fingerprint(),
    );
    let mut upstream = UpstreamConnectionManager::new_authenticated_for_test(
        "worker-session",
        "alice-worker",
        worker.local_schema_fingerprint(),
        worker.local_policy_fingerprint(),
        "alice",
    );

    let server_messages = upstream
        .receive(&mut worker, downstream.open().unwrap())
        .unwrap();
    downstream.receive(&mut worker, server_messages).unwrap();

    let server_messages = upstream
        .receive(
            &mut worker,
            vec![ClientMessage::UploadTx {
                tx: ClientTx {
                    tx_id: existing_tx.clone(),
                    branch_id: None,
                    conflict_mode: TxConflictMode::Mergeable,
                    created_at: 1,
                    author: Some("alice".to_owned()),
                },
                data: vec![ClientDataRecord {
                    table: "projects".to_owned(),
                    row_id: "project-spoof".to_owned(),
                    op: DataOp::Insert,
                    values: BTreeMap::from([("title".to_owned(), json!("Spoof"))]),
                }],
                reads: Vec::new(),
            }],
        )
        .unwrap();

    assert!(server_messages.iter().any(|message| {
        matches!(message, ServerMessage::UploadAck { tx_id } if tx_id == &existing_tx)
    }));
    assert!(server_messages.iter().any(|message| {
        matches!(
            message,
            ServerMessage::TxStatus {
                tx_id,
                status: TxStatusKind::EdgeAccepted
            } if tx_id == &existing_tx
        )
    }));
    assert_eq!(
        row_ids(worker.read_rows("projects").unwrap()),
        vec!["project-existing"]
    );
}

#[test]
fn upstream_exclusive_upload_without_write_reads_is_rejected() {
    let harness = Harness::new();
    let mut worker = harness.memory("alice-worker", "alice").unwrap();
    let mut downstream = DownstreamConnectionManager::new(
        "tab-session",
        "alice-tab",
        worker.local_schema_fingerprint(),
        worker.local_policy_fingerprint(),
    );
    let mut upstream = UpstreamConnectionManager::new_authenticated_for_test(
        "worker-session",
        "alice-worker",
        worker.local_schema_fingerprint(),
        worker.local_policy_fingerprint(),
        "alice",
    );

    let server_messages = upstream
        .receive(&mut worker, downstream.open().unwrap())
        .unwrap();
    downstream.receive(&mut worker, server_messages).unwrap();

    let tx_id = "tx-alice-tab-9001".to_owned();
    let server_messages = upstream
        .receive(
            &mut worker,
            vec![ClientMessage::UploadTx {
                tx: ClientTx {
                    tx_id: tx_id.clone(),
                    branch_id: None,
                    conflict_mode: TxConflictMode::Exclusive,
                    created_at: 1,
                    author: Some("alice".to_owned()),
                },
                data: vec![ClientDataRecord {
                    table: "projects".to_owned(),
                    row_id: "project-exclusive-no-reads".to_owned(),
                    op: DataOp::Insert,
                    values: BTreeMap::from([("title".to_owned(), json!("No reads"))]),
                }],
                reads: Vec::new(),
            }],
        )
        .unwrap();

    assert!(server_messages.iter().any(|message| {
        matches!(
            message,
            ServerMessage::TxStatus {
                tx_id: status_tx_id,
                status: TxStatusKind::Rejected { code, .. }
            } if status_tx_id == &tx_id && code == "upload_rejected"
        )
    }));
    assert!(worker.transaction_info(&tx_id).is_err());
    assert!(worker.read_rows("projects").unwrap().is_empty());
}

#[test]
fn rejected_upload_tx_does_not_leave_branch_or_row_id_side_effects() {
    let harness = Harness::new();
    let mut worker = harness.memory("alice-worker", "alice").unwrap();
    let branches_before = worker.branches().unwrap();
    let mut downstream = DownstreamConnectionManager::new(
        "tab-session",
        "alice-tab",
        worker.local_schema_fingerprint(),
        worker.local_policy_fingerprint(),
    );
    let mut upstream = UpstreamConnectionManager::new_authenticated_for_test(
        "worker-session",
        "alice-worker",
        worker.local_schema_fingerprint(),
        worker.local_policy_fingerprint(),
        "alice",
    );

    let server_messages = upstream
        .receive(&mut worker, downstream.open().unwrap())
        .unwrap();
    downstream.receive(&mut worker, server_messages).unwrap();

    let row_id = "project-duplicate-upload";
    let tx_id = "tx-alice-tab-9002".to_owned();
    let duplicate = ClientDataRecord {
        table: "projects".to_owned(),
        row_id: row_id.to_owned(),
        op: DataOp::Insert,
        values: BTreeMap::from([("title".to_owned(), json!("Duplicate"))]),
    };
    let server_messages = upstream
        .receive(
            &mut worker,
            vec![ClientMessage::UploadTx {
                tx: ClientTx {
                    tx_id: tx_id.clone(),
                    branch_id: Some("draft-side-effect".to_owned()),
                    conflict_mode: TxConflictMode::Mergeable,
                    created_at: 1,
                    author: Some("alice".to_owned()),
                },
                data: vec![duplicate.clone(), duplicate],
                reads: Vec::new(),
            }],
        )
        .unwrap();

    assert!(server_messages.iter().any(|message| {
        matches!(
            message,
            ServerMessage::TxStatus {
                tx_id: status_tx_id,
                status: TxStatusKind::Rejected { code, .. }
            } if status_tx_id == &tx_id && code == "upload_rejected"
        )
    }));
    assert_eq!(worker.branches().unwrap(), branches_before);
    assert!(!worker.row_id_exists_for_test(row_id).unwrap());
}

#[test]
fn rejected_exclusive_upload_reads_do_not_leave_row_id_side_effects() {
    let harness = Harness::new();
    let mut worker = harness.memory("alice-worker", "alice").unwrap();
    let mut downstream = DownstreamConnectionManager::new(
        "tab-session",
        "alice-tab",
        worker.local_schema_fingerprint(),
        worker.local_policy_fingerprint(),
    );
    let mut upstream = UpstreamConnectionManager::new_authenticated_for_test(
        "worker-session",
        "alice-worker",
        worker.local_schema_fingerprint(),
        worker.local_policy_fingerprint(),
        "alice",
    );

    let server_messages = upstream
        .receive(&mut worker, downstream.open().unwrap())
        .unwrap();
    downstream.receive(&mut worker, server_messages).unwrap();

    let tx_id = "tx-alice-tab-9003".to_owned();
    let data_row_id = "project-exclusive-rollback";
    let extra_read_row_id = "project-exclusive-extra-read";
    let server_messages = upstream
        .receive(
            &mut worker,
            vec![ClientMessage::UploadTx {
                tx: ClientTx {
                    tx_id: tx_id.clone(),
                    branch_id: None,
                    conflict_mode: TxConflictMode::Exclusive,
                    created_at: 1,
                    author: Some("alice".to_owned()),
                },
                data: vec![ClientDataRecord {
                    table: "projects".to_owned(),
                    row_id: data_row_id.to_owned(),
                    op: DataOp::Insert,
                    values: BTreeMap::from([("title".to_owned(), json!("Rollback"))]),
                }],
                reads: vec![
                    ReadRecord {
                        tx_id: tx_id.clone(),
                        table: "projects".to_owned(),
                        row_id: data_row_id.to_owned(),
                        reason: 3,
                        observed_tx_id: None,
                    },
                    ReadRecord {
                        tx_id: tx_id.clone(),
                        table: "projects".to_owned(),
                        row_id: extra_read_row_id.to_owned(),
                        reason: 2,
                        observed_tx_id: Some("tx-missing-node-1".to_owned()),
                    },
                ],
            }],
        )
        .unwrap();

    assert!(server_messages.iter().any(|message| {
        matches!(
            message,
            ServerMessage::TxStatus {
                tx_id: status_tx_id,
                status: TxStatusKind::Rejected { code, .. }
            } if status_tx_id == &tx_id && code == "upload_rejected"
        )
    }));
    assert!(!worker.row_id_exists_for_test(data_row_id).unwrap());
    assert!(!worker.row_id_exists_for_test(extra_read_row_id).unwrap());
}

#[test]
fn upload_delete_with_values_returns_rejection_status() {
    let harness = Harness::new();
    let mut edge = harness.memory("edge", "service").unwrap();
    let mut upstream = UpstreamConnectionManager::new_authenticated_for_test(
        "edge-session",
        "edge",
        edge.local_schema_fingerprint(),
        edge.local_policy_fingerprint(),
        "alice",
    );
    let schema_fingerprint = edge.local_schema_fingerprint();
    let policy_fingerprint = edge.local_policy_fingerprint();
    let tx_id = "tx-alice-tab-9010".to_owned();

    let server_messages = upstream
        .receive(
            &mut edge,
            vec![
                ClientMessage::Hello(ClientHello {
                    protocol_version: ProtocolVersion(2),
                    session_id: SessionId::new("tab-session"),
                    node_id: "alice-tab".to_owned(),
                    schema_fingerprint,
                    policy_fingerprint,
                }),
                ClientMessage::UploadTx {
                    tx: ClientTx {
                        tx_id: tx_id.clone(),
                        branch_id: None,
                        conflict_mode: TxConflictMode::Mergeable,
                        created_at: 1,
                        author: None,
                    },
                    data: vec![ClientDataRecord {
                        table: "todos".to_owned(),
                        row_id: "todo-delete-bad".to_owned(),
                        op: DataOp::Delete,
                        values: BTreeMap::from([("title".to_owned(), json!("bad"))]),
                    }],
                    reads: Vec::new(),
                },
            ],
        )
        .unwrap();

    assert!(!upstream.is_closed());
    assert!(server_messages.iter().any(|message| {
        matches!(message, ServerMessage::UploadAck { tx_id: acked } if acked == &tx_id)
    }));
    assert!(server_messages.iter().any(|message| {
        matches!(
            message,
            ServerMessage::TxStatus {
                tx_id: status_tx_id,
                status: TxStatusKind::Rejected { code, .. }
            } if status_tx_id == &tx_id && code == "upload_rejected"
        )
    }));
}

#[test]
fn upload_insert_missing_required_field_returns_rejection_status() {
    let harness = Harness::new();
    let mut edge = harness.memory("edge", "service").unwrap();
    let mut upstream = UpstreamConnectionManager::new_authenticated_for_test(
        "edge-session",
        "edge",
        edge.local_schema_fingerprint(),
        edge.local_policy_fingerprint(),
        "alice",
    );
    let schema_fingerprint = edge.local_schema_fingerprint();
    let policy_fingerprint = edge.local_policy_fingerprint();
    let tx_id = "tx-alice-tab-9011".to_owned();

    let server_messages = upstream
        .receive(
            &mut edge,
            vec![
                ClientMessage::Hello(ClientHello {
                    protocol_version: ProtocolVersion(2),
                    session_id: SessionId::new("tab-session"),
                    node_id: "alice-tab".to_owned(),
                    schema_fingerprint,
                    policy_fingerprint,
                }),
                ClientMessage::UploadTx {
                    tx: ClientTx {
                        tx_id: tx_id.clone(),
                        branch_id: None,
                        conflict_mode: TxConflictMode::Mergeable,
                        created_at: 1,
                        author: None,
                    },
                    data: vec![ClientDataRecord {
                        table: "todos".to_owned(),
                        row_id: "todo-missing-field".to_owned(),
                        op: DataOp::Insert,
                        values: BTreeMap::from([("title".to_owned(), json!("Missing project"))]),
                    }],
                    reads: Vec::new(),
                },
            ],
        )
        .unwrap();

    assert!(!upstream.is_closed());
    assert!(server_messages.iter().any(|message| {
        matches!(
            message,
            ServerMessage::TxStatus {
                tx_id: status_tx_id,
                status: TxStatusKind::Rejected { code, .. }
            } if status_tx_id == &tx_id && code == "upload_rejected"
        )
    }));
}

#[test]
fn upload_system_field_returns_rejection_status() {
    let harness = Harness::new();
    let mut edge = harness.memory("edge", "service").unwrap();
    let mut upstream = UpstreamConnectionManager::new_authenticated_for_test(
        "edge-session",
        "edge",
        edge.local_schema_fingerprint(),
        edge.local_policy_fingerprint(),
        "alice",
    );
    let schema_fingerprint = edge.local_schema_fingerprint();
    let policy_fingerprint = edge.local_policy_fingerprint();
    let tx_id = "tx-alice-tab-9012".to_owned();

    let server_messages = upstream
        .receive(
            &mut edge,
            vec![
                ClientMessage::Hello(ClientHello {
                    protocol_version: ProtocolVersion(2),
                    session_id: SessionId::new("tab-session"),
                    node_id: "alice-tab".to_owned(),
                    schema_fingerprint,
                    policy_fingerprint,
                }),
                ClientMessage::UploadTx {
                    tx: ClientTx {
                        tx_id: tx_id.clone(),
                        branch_id: None,
                        conflict_mode: TxConflictMode::Mergeable,
                        created_at: 1,
                        author: None,
                    },
                    data: vec![ClientDataRecord {
                        table: "projects".to_owned(),
                        row_id: "project-system-field".to_owned(),
                        op: DataOp::Insert,
                        values: BTreeMap::from([
                            ("title".to_owned(), json!("System field")),
                            ("j_created_at".to_owned(), json!(1)),
                        ]),
                    }],
                    reads: Vec::new(),
                },
            ],
        )
        .unwrap();

    assert!(!upstream.is_closed());
    assert!(server_messages.iter().any(|message| {
        matches!(
            message,
            ServerMessage::TxStatus {
                tx_id: status_tx_id,
                status: TxStatusKind::Rejected {
                    code,
                    detail: Some(detail)
                }
            } if status_tx_id == &tx_id
                && code == "upload_rejected"
                && detail["message"].as_str().is_some_and(|message| {
                    message.contains("system fields")
                })
        )
    }));
}

#[test]
fn connection_manager_rejects_upload_ack_before_handshake_without_marking_ack() {
    let harness = Harness::new();
    let mut tab = harness.memory("alice-tab", "alice").unwrap();
    tab.create_project("project-1", "Upload plan").unwrap();
    let mut downstream = DownstreamConnectionManager::new(
        "tab-session",
        "alice-tab",
        tab.local_schema_fingerprint(),
        tab.local_policy_fingerprint(),
    );

    let _open_messages = downstream.open().unwrap();
    let result = downstream.receive(
        &mut tab,
        vec![ServerMessage::UploadAck {
            tx_id: "tx-alice-tab-1".to_owned(),
        }],
    );

    assert!(result.is_err());
    assert!(downstream.is_closed());
    assert_eq!(tab.upload_ack_count_for_test().unwrap(), 0);
}

#[test]
fn connection_manager_ignores_upload_ack_for_not_in_flight_tx() {
    let harness = Harness::new();
    let mut tab = harness.memory("alice-tab", "alice").unwrap();
    let mut worker = harness.memory("alice-worker", "alice").unwrap();
    tab.create_project("project-1", "Upload plan").unwrap();
    tab.create_todo("todo-upload-loop", "Flush after hello", false, "project-1")
        .unwrap();

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
    let upload_messages = downstream.receive(&mut tab, server_messages).unwrap();
    assert!(upload_messages
        .iter()
        .any(|message| matches!(message, ClientMessage::UploadTx { .. })));

    downstream
        .receive(
            &mut tab,
            vec![ServerMessage::UploadAck {
                tx_id: "unknown-upload-tx".to_owned(),
            }],
        )
        .unwrap();

    assert_eq!(tab.upload_ack_count_for_test().unwrap(), 0);
}

#[test]
fn connection_manager_ack_frees_slot_without_resending_acked_upload() {
    let harness = Harness::new();
    let mut tab = harness.memory("alice-tab", "alice").unwrap();
    let mut worker = harness.memory("alice-worker", "alice").unwrap();
    tab.create_project("project-1", "Upload plan").unwrap();
    tab.create_todo("todo-upload-next", "Next after ack", false, "project-1")
        .unwrap();

    let mut downstream = DownstreamConnectionManager::new(
        "tab-session",
        "alice-tab",
        tab.local_schema_fingerprint(),
        tab.local_policy_fingerprint(),
    );
    downstream.set_max_in_flight_uploads_for_test(1);
    let mut upstream = UpstreamConnectionManager::new(
        "worker-session",
        "alice-worker",
        worker.local_schema_fingerprint(),
        worker.local_policy_fingerprint(),
    );

    let server_messages = upstream
        .receive(&mut worker, downstream.open().unwrap())
        .unwrap();
    let first_messages = downstream.receive(&mut tab, server_messages).unwrap();
    let first_uploads = upload_tx_ids(&first_messages);
    assert_eq!(first_uploads.len(), 1);
    let first_tx_id = first_uploads[0].clone();

    let second_messages = downstream
        .receive(
            &mut tab,
            vec![ServerMessage::UploadAck {
                tx_id: first_tx_id.clone(),
            }],
        )
        .unwrap();
    let second_uploads = upload_tx_ids(&second_messages);

    assert_eq!(tab.upload_ack_count_for_test().unwrap(), 1);
    assert_eq!(second_uploads.len(), 1);
    assert_ne!(second_uploads[0], first_tx_id);
}

#[test]
fn connection_manager_replays_acked_incomplete_upload_after_reconnect() {
    let harness = Harness::new();
    let mut tab = harness.memory("alice-tab", "alice").unwrap();
    let mut worker = harness.memory("alice-worker", "alice").unwrap();
    let tx_id = tab
        .create_project("project-reconnect", "Replay me")
        .unwrap();

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
    let first_messages = downstream.receive(&mut tab, server_messages).unwrap();
    assert_eq!(upload_tx_ids(&first_messages), vec![tx_id.clone()]);

    let after_ack = downstream
        .receive(
            &mut tab,
            vec![ServerMessage::UploadAck {
                tx_id: tx_id.clone(),
            }],
        )
        .unwrap();
    assert!(upload_tx_ids(&after_ack).is_empty());
    assert_eq!(tab.upload_ack_count_for_test().unwrap(), 1);

    let mut reconnected_upstream = UpstreamConnectionManager::new(
        "worker-session-reconnected",
        "alice-worker",
        worker.local_schema_fingerprint(),
        worker.local_policy_fingerprint(),
    );
    let server_messages = reconnected_upstream
        .receive(&mut worker, downstream.open().unwrap())
        .unwrap();
    let replay_messages = downstream.receive(&mut tab, server_messages).unwrap();

    assert_eq!(upload_tx_ids(&replay_messages), vec![tx_id]);
}

#[test]
fn upstream_connection_manager_can_refresh_active_subscriptions() {
    let harness = Harness::new();
    let mut tab = harness.memory("alice-tab", "alice").unwrap();
    let mut worker = harness.memory("alice-worker", "alice").unwrap();
    worker.create_project("project-1", "Refresh data").unwrap();

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
    let query = open_todos_query();
    let (subscription, subscribe_messages) = downstream
        .subscribe(query.clone(), SettlementTier::Local)
        .unwrap();

    let server_messages = upstream
        .receive(&mut worker, downstream.open().unwrap())
        .unwrap();
    let client_messages = downstream.receive(&mut tab, server_messages).unwrap();
    let server_messages = upstream
        .receive(&mut worker, [client_messages, subscribe_messages].concat())
        .unwrap();
    downstream.receive(&mut tab, server_messages).unwrap();

    worker
        .create_todo(
            "todo-refresh",
            "Refresh active subscription",
            false,
            "project-1",
        )
        .unwrap();

    let server_messages = upstream.refresh_active_subscriptions(&worker).unwrap();
    let client_messages = downstream.receive(&mut tab, server_messages).unwrap();

    assert!(client_messages.iter().any(|message| {
        matches!(
            message,
            ClientMessage::Ack {
                cursor: Some(_),
                ..
            }
        )
    }));
    assert!(downstream.is_settled(&subscription, SettlementTier::Local));
    assert_eq!(row_ids(tab.query(query).unwrap()), vec!["todo-refresh"]);
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

    let [ClientMessage::Unsubscribe { subscription_id }] = unsubscribe_messages.as_slice() else {
        panic!("expected one unsubscribe message");
    };
    assert_eq!(subscription_id, subscription.id());
    assert!(client_messages.is_empty());
    assert!(!downstream.is_closed());
    assert!(tab.query(query).unwrap().is_empty());
}

#[test]
fn connection_manager_unsubscribe_sends_targeted_unsubscribe_upstream() {
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

    let unsubscribe_messages = downstream.unsubscribe(&dropped_subscription).unwrap();

    let [ClientMessage::Unsubscribe { subscription_id }] = unsubscribe_messages.as_slice() else {
        panic!("expected one unsubscribe message");
    };
    assert_eq!(subscription_id, dropped_subscription.id());

    upstream.receive(&mut worker, unsubscribe_messages).unwrap();

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

fn upload_tx_ids(messages: &[ClientMessage]) -> Vec<String> {
    messages
        .iter()
        .filter_map(|message| match message {
            ClientMessage::UploadTx { tx, .. } => Some(tx.tx_id.clone()),
            _ => None,
        })
        .collect()
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

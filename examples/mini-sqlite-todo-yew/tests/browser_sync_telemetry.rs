use mini_sqlite_todo_yew::{
    browser_telemetry::{otlp_log_payload, BrowserTelemetryConfig, BrowserTelemetryLog},
    native_sync::{NativeSyncLogContext, NativeSyncProbe},
};

#[test]
fn browser_log_payload_uses_session_probe_attributes_and_full_body() {
    let config = BrowserTelemetryConfig {
        endpoint: "http://127.0.0.1:54418".to_owned(),
        service_name: "mini-sqlite-todo-yew-browser".to_owned(),
        service_version: "0.1.0".to_owned(),
        browser_instance_id: "browser-yew-alice".to_owned(),
        deployment_environment: "local".to_owned(),
    };
    let probe = NativeSyncProbe {
        probe_id: "probe-1".to_owned(),
        operation: "insert".to_owned(),
        table: "todos".to_owned(),
        row_id: "todo-123".to_owned(),
        origin_browser_id: "browser-yew-alice".to_owned(),
    };
    let log = BrowserTelemetryLog::new(
        "todo.action.start",
        123_000_000,
        Some(&NativeSyncLogContext {
            session_id: Some("server-session-1".to_owned()),
            probe: Some(probe),
        }),
        r#"{"event":"todo.action.start","title":"Buy milk"}"#,
        [("sync.phase", "local_action")],
    );

    let payload = otlp_log_payload(&config, log);
    let encoded = serde_json::to_string(&payload).unwrap();

    assert!(encoded.contains("resourceLogs"));
    assert!(encoded.contains("mini-sqlite-todo-yew-browser"));
    assert!(encoded.contains("browser-yew-alice"));
    assert!(encoded.contains("server-session-1"));
    assert!(encoded.contains("probe-1"));
    assert!(encoded.contains("insert"));
    assert!(encoded.contains("todos"));
    assert!(encoded.contains("todo-123"));
    assert!(encoded.contains("Buy milk"));
    assert!(!encoded.contains("traceId"));
}

#[test]
fn browser_log_payload_allows_logs_before_server_session_is_known() {
    let config = BrowserTelemetryConfig {
        endpoint: "http://127.0.0.1:54418".to_owned(),
        service_name: "mini-sqlite-todo-yew-browser".to_owned(),
        service_version: "0.1.0".to_owned(),
        browser_instance_id: "browser-yew-alice".to_owned(),
        deployment_environment: "local".to_owned(),
    };
    let context = NativeSyncLogContext {
        session_id: None,
        probe: None,
    };
    let log = BrowserTelemetryLog::new(
        "sync.client.frame_sent",
        123_000_000,
        Some(&context),
        r#"{"event":"sync.client.frame_sent"}"#,
        [("sync.phase", "client_frame_sent")],
    );

    let encoded = serde_json::to_string(&otlp_log_payload(&config, log)).unwrap();

    assert!(encoded.contains("sync.client.frame_sent"));
    assert!(!encoded.contains("sync.session_id"));
}

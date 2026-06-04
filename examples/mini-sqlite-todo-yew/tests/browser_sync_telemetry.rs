use mini_sqlite_todo_yew::{
    browser_telemetry::{BrowserTelemetryConfig, BrowserTelemetrySpan, SpanKind},
    native_sync::{NativeSyncProbe, NativeTraceContext},
};

#[test]
fn browser_trace_payload_uses_safe_probe_attributes() {
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
    let span = BrowserTelemetrySpan::from_trace_context(
        "todo.action.start",
        SpanKind::Internal,
        123_000_000,
        124_000_000,
        &NativeTraceContext {
            traceparent: "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01".to_owned(),
            probe: Some(probe),
        },
        [("sync.phase", "local_action")],
    )
    .expect("valid trace context");

    let payload = mini_sqlite_todo_yew::browser_telemetry::otlp_trace_payload(&config, span);
    let encoded = serde_json::to_string(&payload).unwrap();

    assert!(encoded.contains("mini-sqlite-todo-yew-browser"));
    assert!(encoded.contains("browser-yew-alice"));
    assert!(encoded.contains("4bf92f3577b34da6a3ce929d0e0e4736"));
    assert!(encoded.contains("probe-1"));
    assert!(encoded.contains("insert"));
    assert!(encoded.contains("todos"));
    assert!(encoded.contains("todo-123"));
    assert!(!encoded.contains("Buy milk"));
}

#[test]
fn browser_trace_context_rejects_invalid_traceparent() {
    let context = NativeTraceContext {
        traceparent: "not-a-traceparent".to_owned(),
        probe: None,
    };

    let span = BrowserTelemetrySpan::from_trace_context(
        "todo.action.start",
        SpanKind::Internal,
        123_000_000,
        124_000_000,
        &context,
        [],
    );

    assert!(span.is_none());
}

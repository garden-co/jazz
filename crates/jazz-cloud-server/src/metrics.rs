#[cfg(feature = "otel")]
mod inner {
    use opentelemetry::{
        KeyValue,
        metrics::{Counter, Gauge, Histogram, UpDownCounter},
    };

    pub struct SyncMetrics {
        // Connection metrics
        pub connections_active: UpDownCounter<i64>,
        pub connections_total: Counter<u64>,
        // Message throughput
        pub messages_sent: Counter<u64>,
        pub messages_received: Counter<u64>,
        pub message_size_bytes: Histogram<f64>,
        // Subscription metrics
        pub subscriptions_total: Counter<u64>,
        // Persistence & durability
        pub persistence_acks_total: Counter<u64>,
        pub query_settled_total: Counter<u64>,
        // Errors & warnings
        pub errors_total: Counter<u64>,
        pub schema_warnings_total: Counter<u64>,
        // Sync handler
        pub handler_duration_ms: Histogram<f64>,
        // Broadcast
        pub broadcast_lag_events: Counter<u64>,
        // Worker metrics
        pub worker_command_duration_ms: Histogram<f64>,
        pub worker_commands_total: Counter<u64>,
        pub worker_queue_depth: Gauge<i64>,
        pub worker_apps_active: UpDownCounter<i64>,
        pub app_runtime_created: Counter<u64>,
        // HTTP server metrics
        pub http_active_requests: UpDownCounter<i64>,
        pub http_request_duration: Histogram<f64>,
    }

    impl SyncMetrics {
        pub fn new() -> Self {
            let meter = opentelemetry::global::meter("jazz-cloud-server");
            Self {
                connections_active: meter
                    .i64_up_down_counter("jazz.sync.connections.active")
                    .build(),
                connections_total: meter.u64_counter("jazz.sync.connections.total").build(),
                messages_sent: meter.u64_counter("jazz.sync.messages.sent").build(),
                messages_received: meter.u64_counter("jazz.sync.messages.received").build(),
                message_size_bytes: meter.f64_histogram("jazz.sync.message.size_bytes").build(),
                subscriptions_total: meter.u64_counter("jazz.sync.subscriptions.total").build(),
                persistence_acks_total: meter
                    .u64_counter("jazz.sync.persistence_acks.total")
                    .build(),
                query_settled_total: meter.u64_counter("jazz.sync.query_settled.total").build(),
                errors_total: meter.u64_counter("jazz.sync.errors.total").build(),
                schema_warnings_total: meter.u64_counter("jazz.sync.schema_warnings.total").build(),
                handler_duration_ms: meter.f64_histogram("jazz.sync.handler.duration_ms").build(),
                broadcast_lag_events: meter.u64_counter("jazz.sync.broadcast.lag_events").build(),
                worker_command_duration_ms: meter
                    .f64_histogram("jazz.worker.command.duration_ms")
                    .build(),
                worker_commands_total: meter.u64_counter("jazz.worker.commands.total").build(),
                worker_queue_depth: meter.i64_gauge("jazz.worker.queue.depth").build(),
                worker_apps_active: meter.i64_up_down_counter("jazz.worker.apps.active").build(),
                app_runtime_created: meter.u64_counter("jazz.app.runtime.created").build(),
                http_active_requests: meter
                    .i64_up_down_counter("http.server.active_requests")
                    .build(),
                http_request_duration: meter.f64_histogram("http.server.request.duration").build(),
            }
        }
    }

    /// Guard that decrements active connection counter on drop.
    pub struct ConnectionMetricsGuard {
        pub metrics: std::sync::Arc<SyncMetrics>,
        pub attrs: Vec<KeyValue>,
    }

    impl Drop for ConnectionMetricsGuard {
        fn drop(&mut self) {
            self.metrics.connections_active.add(-1, &self.attrs);
        }
    }
}

#[cfg(feature = "otel")]
pub use inner::*;

#[cfg(not(feature = "otel"))]
mod noop {
    #[allow(dead_code)]
    pub struct SyncMetrics;
    #[allow(dead_code)]
    impl SyncMetrics {
        pub fn new() -> Self {
            Self
        }
    }
}

#[cfg(not(feature = "otel"))]
#[allow(unused_imports)]
pub use noop::*;

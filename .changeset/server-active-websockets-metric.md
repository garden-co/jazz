---
"jazz-tools": patch
---

Add a `jazz.server.active_websockets` OpenTelemetry gauge reporting the server's current inbound WebSocket connection count. It is exported over OTLP only when the crate is built with the `otel` feature and `OTEL_EXPORTER_OTLP_ENDPOINT` is set; builds without the feature are unaffected.

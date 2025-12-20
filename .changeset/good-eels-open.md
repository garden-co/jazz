---
"jazz-tools": patch
"cojson": patch
---

Added OpenTelemetry observability support for monitoring subscription performance

- Instrumented `SubscriptionScope` with metrics (`jazz.subscription.active`, `jazz.subscription.first_load`) and tracing spans
- Added performance tracking for subscription lifecycle events including storage loading, peer fetching, and transaction parsing
- Added new "Perf" tab in Jazz Inspector to visualize subscription load times and metrics
- Added documentation for integrating Jazz with OpenTelemetry-compatible observability backends


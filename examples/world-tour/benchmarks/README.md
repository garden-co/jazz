# WorldTour benchmark variant

This native package duplicates the WorldTour schema subset used by its
deterministic fixture. It measures the Vue app's two calendar browse paths:
the member itinerary and the public confirmed-only itinerary. Both are
date-ordered three-week windows, capped at 12 rows, with their venue relation
included. The map consumes those included stops client-side; it does not issue
a separate viewport query. This package deliberately does not import a
frontend runtime or share fixture code with the app.

```sh
cargo test -p jazz-example-world-tour-benchmark
cargo bench -p jazz-example-world-tour-benchmark --bench queries
```

The fixture is deterministic. Topology/reconnect, offline-edit, and concurrent
schedule-conflict coverage belong to the app-facing E2E suite, where the same
schema/query shapes can be exercised over real clients and servers.

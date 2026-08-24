# WorldTour benchmark variant

This native package duplicates the WorldTour schema subset used by its
deterministic fixture. It measures the two high-frequency browse paths: the Vue app's bounded,
date-ordered three-week itinerary window (limited to 12 rows), and a
latitude-bounded map viewport. It deliberately does not import a frontend
runtime or share fixture code with the app.

```sh
cargo test -p jazz-example-world-tour-benchmark
cargo bench -p jazz-example-world-tour-benchmark --bench queries
```

The fixture is deterministic. Topology/reconnect, offline-edit, and concurrent
schedule-conflict coverage belong to the app-facing E2E suite, where the same
schema/query shapes can be exercised over real clients and servers.

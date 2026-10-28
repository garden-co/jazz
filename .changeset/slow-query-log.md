---
"jazz-tools": patch
---

Jazz now logs slow queries on every host: the server, browser tabs and workers, React Native, Node backends, and the Rust client. Each warning names the app and one query shape that held the database for 50 ms or more, with how often it ran, how long it took in all, how many rows it scanned, SQLite's query plan, and which index would likely make it fast. The first slow shape is reported at once, later ones at most every 10 seconds. JavaScript clients write it with `console.warn`; the server and the Rust client write it with `tracing`. Change the threshold with `slowQueryThresholdMs` in the client config (or `JazzClient::set_slow_query_threshold` in Rust, `JAZZ_SLOW_QUERY_MS` on the server); `0` turns the log off.

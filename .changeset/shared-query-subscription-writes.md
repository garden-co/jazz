---
"jazz-tools": patch
"jazz-napi": patch
"jazz-wasm": patch
"jazz-rn": patch
---

Make writes cheaper when many subscriptions share one query shape with different parameters. A write now only evaluates the subscriptions whose parameters it touches, writes to tables no subscription reads no longer scale with the number of live subscriptions, and one-shot queries and unsubscribes no longer walk the whole subscription graph. Attaching another subscription to a query shape that is already live is incremental, so it costs about the same as the first instead of growing with the number of subscriptions already open.

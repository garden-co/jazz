---
"jazz-tools": patch
"jazz-napi": patch
"jazz-wasm": patch
"jazz-rn": patch
---

Make writes cheaper when many subscriptions share one query shape with different parameters. A write now only evaluates the subscriptions whose parameters it touches, writes to tables no subscription reads no longer scale with the number of live subscriptions, and one-shot queries and unsubscribes no longer walk the whole subscription graph. Top-N queries grouped by a non-unique column now keep the correct rows per group.

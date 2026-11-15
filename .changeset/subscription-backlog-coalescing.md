---
"jazz-tools": patch
---

Subscriptions now have a bounded backlog. If a subscriber falls far behind a fast writer, its undelivered changes are replaced by one update of the current result, so its memory no longer grows without limit. That happens once more than 10000 changed rows (by default) are waiting.

- **TypeScript:** the update has `reason: "backlog"` in `onUpdate`'s second argument. Set the limit with `subscriptionBacklogRows` in the client config, or per query with `backlogRows`. It must be at least 1 and cannot be turned off.
- **Rust:** `jazz::tools` and `jazz-app` deltas carry `coalesced: true`. Set the limit with `Db::set_subscription_backlog_limit` and `ReadOpts::with_backlog_rows`.
- **Public Rust struct literals:** the `jazz::tools` and `jazz-app` subscription streams now hold a bounded number of items. `OrderedRowDelta` and `jazz_app::Delta` have a new `coalesced` field, so code that builds them with struct literals needs to set it.

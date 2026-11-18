---
"jazz-tools": minor
---

Subscriptions now have a bounded backlog. If a subscriber falls more than 10000 changed rows behind (the default), its undelivered changes are replaced by one update of the current result, so its memory no longer grows without limit. A subscriber that keeps up is never affected, even by one large write such as a bulk import.

- **TypeScript:** the update has `reason: "backlog"` in `onUpdate`'s second argument. Set the limit with `subscriptionBacklogRows` in the client config, or per query with `backlogRows`. It must be at least 1 and cannot be turned off. React Native ignores `subscriptionBacklogRows` for now; per-query `backlogRows` still applies there.
- **Rust:** `Db::set_subscription_backlog_limit` and `ReadOpts::with_backlog_rows` set the limit. The `jazz::tools` and `jazz-app` subscription streams now hold at most 16 items; past that, the backlog waits in the core.
- **Breaking changes in public Rust types:**
  - `SubscriptionEvent::Delta` has a new `coalesced` field, so exhaustive matches on it need `..` or the field.
  - `OrderedRowDelta`, `jazz_app::Delta` and `ReadOpts` have new fields (`coalesced`, `coalesced` and `backlog_rows`), so code that builds them with struct literals needs to set them or use `..Default::default()`.

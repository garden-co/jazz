---
"jazz-tools": patch
---

**Breaking (Rust): subscription deltas carry a `key`; history reads can be subscribed to in `jazz-app-support`.**

Subscribe to history reads, as in TypeScript. `jazz.subscribe(todos::history().id(id))` streams each accepted version as a row of its own, and new versions arrive as they are accepted. Subscription deltas now name the result they change with `key` (on `Added`, `Updated` and `Removed`), and `Rows::apply` applies them by key. A live union that holds a row once per matching arm now keeps each occurrence right.

Migration: `Added`, `Updated` and `Removed` gain a `pub key: ResultKey` (re-exported as `jazz_app_support::ResultKey`) and are now `#[non_exhaustive]`. Destructure them with `..` (`Added { row, .. }`); they are delivered by subscriptions and not built by callers.

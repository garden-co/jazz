---
"jazz-tools": patch
"jazz-napi": patch
"jazz-wasm": patch
"jazz-rn": patch
---

Support `.includeDeleted()` in subscriptions for regular and history tables, with both local-first and remote reads. Subscriptions retain deleted rows' content, receive restoration updates, and fetch deleted content from upstream when needed. Server read permissions still apply.

**Breaking wire change:** subscription registrations now carry `ReadViewSpec.include_deleted`. This is part of the upcoming wire v6 break; upgrade clients, relays, and servers together. The protocol version is unchanged by this change. Include-deleted subscriptions have separate coverage keys; existing coverage keys and stored row/version formats are unchanged.

Include-deleted subscriptions of regular tables accept the current source and snapshots (which transaction reads use), and reject branch views with a "not supported yet" error. Plain one-shot include-deleted reads still accept only the current source. History queries are unaffected.

**Breaking Rust API change:** `ReadOpts.include_deleted` is removed. Rust callers now set the flag on the read view: `ReadOpts { read_view: ReadViewSpec { include_deleted: true, ..Default::default() }, ..Default::default() }`.

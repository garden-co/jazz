---
"jazz-tools": patch
"jazz-rn": patch
---

Client-side checks now match the Rust core:

- Writing a `Date` before 1970 to a timestamp column throws ("dates before 1970 are not supported") instead of storing a far-future time.
- UUIDs must be hyphenated `8-4-4-4-12` or 32 plain hex digits; text with dashes elsewhere is refused.
- Table columns named like the store's own row columns (`rid`, `_branch`, `_clock`, `_writer`, `_state`, `_base`, `_touched`, `_partition`) or starting with `__jazz_aggregate_` are refused when the schema is defined, not first when it compiles.
- React Native: a native lifecycle or encode failure no longer turns the pending-upload count off for the session (it reported 0 from then on, so sign-out skipped the unconfirmed-writes warning). Native errors carry the relay status as `error.code`.

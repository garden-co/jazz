---
"jazz-tools": patch
---

Remove the Rust `Db::as_written_mismatches()` diagnostic counter and the unused `FanoutStats::groups` field. Echo regression tests now observe whole-row fetch requests through the sync transport instead of retaining a counter in database execution state.

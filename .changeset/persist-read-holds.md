---
"jazz-tools": patch
---

A durable client now stores which rows each live subscription held beside its resume point. After a restart, a resumed subscription re-checks the rows it no longer holds, so policy support rows the reader lost access to while the app was closed (a deleted membership, say) are dropped. An access loss now forgets only the resume points of the subscriptions that held the lost row, instead of every closed subscription's, so those reopen with a cheap resume rather than a full download. This adds three `_jazz_read_holds*` tables and a nullable `holds_epoch` column on `_jazz_watermarks` to the client store, created on first use.

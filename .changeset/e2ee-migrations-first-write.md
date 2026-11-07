---
"jazz-tools": patch
---

Support explicit encrypted schema migration and stable catalogue identities
through renames. Initialize a legacy plaintext scope on its first encrypted
insert only after authoritative absence checks; accept root, recipients and
data atomically. Standalone retries retain the original logical values and row
identity, while explicit mergeable transactions refuse implicit initialization.
Hidden or inaccessible existing spaces never become a reason to mint new keys.

Resolve managed administration table additions against the full published
schemas, not partial migration witnesses. Adding another encrypted table
preserves existing managed identities and historical ciphertext. Explicit
attempts to recreate an existing table fail before catalogue publication.
Compatible scope and referencing-table renames preserve reference identity in
runtime and TypeScript migration validation, so existing ciphertext remains
readable and searchable. Retargeting encrypted rows to an unrelated scope still requires
authorised client conversion.

Branch-target first writes to an uninitialised space remain unsupported. Their
wait handles now reject before preparing provisional keys, grants or ciphertext,
and caller mutations cannot change a captured branch target. Existing-space
branch writes are unchanged. Public transactions retain the tested first-successful-read cut and subsequent
snapshot consistency; internal retries preserve the original caller context.
Initial prepared writes and their exclusive retries share the originating
per-Db access context, including its session and attribution references, without
restoring the removed ambient operation-context wrappers.

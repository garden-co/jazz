---
"jazz-tools": patch
---

Add `e2ee.groups.allowInsert` and `e2ee.groups.allowUpdate` to the
`definePermissions` callback. Applications declare group creation and
administration predicates once; Jazz generates ordinary policies for signed
membership, rotation, key delivery, recovery, repair requests and current
membership projections. Public predicates expose `id` and `creatorAccountId`
without changing the stored group schema or signed encodings.

Require authenticated, self-authored lifecycle records and retain immutable
group roots and signed history. Current membership updates check authority on
both the existing and proposed group. Missing rules deny the corresponding
operations, and mixing helper-generated policies with explicit policies on
managed group tables rejects. Applications using raw policies retain their
existing configuration when the helper is unused.

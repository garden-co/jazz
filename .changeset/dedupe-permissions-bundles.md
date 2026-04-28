---
"jazz-tools": patch
---

Deduplicate permissions bundle publishes when content is unchanged.

`publish_permissions_bundle` now short-circuits when the proposed `schema_hash` and `permissions` are identical to the current head, mirroring the content-addressed dedup that schemas already enjoy. Repeated identical publishes no longer bump the version, allocate a new bundle object id, or rewrite the catalogue entry. Optimistic concurrency via `expected_parent_bundle_object_id` still rejects stale parents.

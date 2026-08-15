# jazz — Specification · 22. Structured JSON content adapter

`json-v1` is a required manifest record whose tail element type is `Bytes`.
Declare it with `ContentManifestSchema::with_tail_entry_type("json-v1",
`ValueType::Bytes`, entries, bytes)` and `ColumnSchema::content_manifest`.

Create JSON tail entries with `JsonOperation::encode`; it returns the typed
tail value accepted by the schema. Persist the full manifest using
`ContentManifest::into_value`. `ContentManifestRuntime` materializes the
document, merges complete manifests, and exposes named JSON projections via
`index_values_for_cell`.

JSON nodes and array positions have stable identities. The adapter rejects
duplicate or noncommuting operations, and only returns a merge when its full
candidate tails are coherent. Consolidated immutable JSON objects are
domain-scoped content-addressed storage; the bounded tail is the foreground
write frontier.

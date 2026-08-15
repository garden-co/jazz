# jazz — Specification · 20. Stream content adapter

`stream-v1` is declared as one required content-manifest record with a
`Bytes` tail entry type. It represents an immutable byte prefix plus at most
one inline suffix. Construct the column with
`ContentManifestSchema::with_tail_entry_type("stream-v1", ValueType::Bytes, entries, bytes)`
and `ColumnSchema::content_manifest`.

Use `StreamManifestAdapter::empty_manifest` and `append` to produce the next
complete manifest. Store it with `ContentManifest::into_value`; materialize a
full value or a byte range through `ContentManifestRuntime`. The adapter's
`length` index is available through `index_values_for_cell`.

The tail is bounded inline data. An append beyond its adapter limit is
consolidated into immutable stream tree objects and returns an empty tail. A
merge receives complete candidate manifests and rejects incompatible roots or
tails rather than joining independent fields.

# jazz — Specification · 19. Embedded content manifests

## Overview

This chapter specifies the common substrate for large, structured content
stored by a normal Jazz application row. It deliberately has no `snapshot_heads`
table. A content column is one atomic record-shaped value:

```text
{ root: ContentId, editTail: TypedBoundedTail }
```

The row that owns this cell is its mutable identity. A historical row version
therefore names exactly the content snapshot it contained. Copying the manifest
retains a snapshot; referring to the owner row follows future row versions.

Invariant digest:

- `INV-MANIFEST-1`: root and tail are one atomic replicated cell.
- `INV-MANIFEST-2`: immutable ids are canonical, domain-scoped addresses.
- `INV-MANIFEST-3`: immutable insertion is absent-or-identical only.
- `INV-MANIFEST-4`: merge and interior-index consumers receive the complete manifest.

## Details

### 19.1 Schema and codec

`ColumnSchema::content_manifest(name, ContentManifestSchema)` lowers to one
non-null `Bytes` user column. Its schema metadata declares an adapter kind and
non-zero entry/byte bounds for the tail. The stored `JCM1` codec contains the
32-byte root followed by a length-delimited operation vector. It is canonical:
trailing bytes, truncated fields, and bounds violations are rejected.

The adapter kind belongs to the schema, not every row. A dynamically typed
union column is a different feature and must carry its own discriminant.

`INV-MANIFEST-1`: A manifest MUST be authored, retained, transported, and
merged as one ordinary user cell. An implementation MUST NOT independently
choose a root from one concurrent candidate and a tail from another. Today this
is achieved by the existing whole-column LWW behavior; adapters may later
replace that decision only through `ContentManifestAdapter::merge`, which is
passed full manifests.

### 19.2 Immutable identities and storage

`ContentId` is BLAKE3 over the fixed `jazz-content-id-v1` domain separator,
immutable kind (`leaf`, `node`, or `root`), authorization/encryption domain,
adapter kind, and canonical payload length and bytes.

`INV-MANIFEST-2`: Every semantically relevant immutable field, including
adapter/version and domain, MUST be in the canonical payload or fixed preimage.
The authorization/encryption domain MUST be included, so equality does not
leak across domains.

`INV-MANIFEST-3`: Immutable storage MUST implement
`put_if_absent_or_identical`: an existing id with identical canonical bytes is
success; an existing id with different bytes is a hard integrity error. The
foundation exposes this as `ImmutableContentStore`; storage-backed adapters
must implement it before content-addressing is relied upon operationally.

### 19.3 Materialization, merge, and indices

`ContentManifestAdapter` is the adapter boundary. `materialize`, `merge`, and
`index_values` each receive the full `ContentManifest` plus immutable lookup.
The materialization request may be full, range, or named projection; partial
requests are advisory because an adapter may need to load more of its tree to
answer safely.

`INV-MANIFEST-4`: Any merge strategy, interior query, or index that observes
content MUST materialize from a coherent `{root, editTail}` pair. It MUST NOT
read the root while ignoring a live tail.

The foundation intentionally does not install adapter-specific query lowering
or synthetic merge behavior. The four content adapters own their operation
codecs, tree layouts, and conflict semantics. Until an adapter has a safe
full-manifest merge implementation, normal atomic column LWW is the only
available conflict resolution.

### 19.4 Consolidation race (open)

Consolidation turns `{R7, [e...]}` into `{R8, []}`. A concurrent foreground
tail update can otherwise be overwritten by ordinary LWW. This foundation does
not claim to solve that race. Adapters must either perform consolidation in the
same foreground candidate, merge typed operations, or wait for a core
compare-and-swap/expected-manifest primitive. Background consolidation without
one of those rules is forbidden.

## Open questions

- The exact storage transaction that joins immutable `put_if_absent-or-identical`
  with publication of the owning application row is not yet a core primitive.
- Query planner registration and a first-class adapter registry will be added
  with the feature lanes once concrete materializers exist.

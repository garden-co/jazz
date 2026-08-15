# jazz — Specification · 19. Embedded content manifests

## Overview

This chapter specifies the common substrate for large, structured content
stored by a normal Jazz application row. A content column is one atomic typed
record value:

```text
{ root: ContentId, editTail: TypedBoundedTail }
```

The row that owns this cell is its mutable identity. A historical row version
therefore names exactly the content snapshot it contained. Copying the manifest
retains a snapshot; referring to the owner row follows future row versions.

Invariant digest:

- `INV-CONTENT-1`: root and tail are one atomic replicated cell.
- `INV-CONTENT-2`: immutable ids are canonical, domain-scoped addresses.
- `INV-CONTENT-3`: immutable insertion is absent-or-identical only.
- `INV-CONTENT-4`: merge and interior-index consumers receive the complete manifest.

## Details

### 19.1 Schema and codec

`ColumnSchema::content_manifest(name, ContentManifestSchema)` lowers to one
non-null `Record` user column with exactly these ordered fields: `root: Bytes`
then `editTail: Array<T>`. `T` is declared by the content variant in the schema;
for example, text may use a typed edit record while a stream may use an append
operation record. The ordinary Groove record codec validates the nested layout
and canonical encoding, and the content boundary additionally requires a
32-byte root plus non-zero entry/byte bounds for the tail. The byte bound is
the sum of each typed tail entry's canonical encoded size.

The adapter kind belongs to the schema, not every row. A dynamically typed
union column is a different feature and must carry its own discriminant.

Every schema uses the explicit `JAZZ-CONTENT-MANIFEST-SCHEMA-V2` envelope. It
carries the tail entry type and verifies exact equality with the inline
`editTail` element type. V1 and bare legacy schema bytes are rejected. There
is no released content-cell persistence format to migrate, so V2 is the only
supported wire and authoring format for this alpha.

`INV-CONTENT-1`: A manifest MUST be authored, retained, transported, and
merged as one ordinary user cell. An implementation MUST NOT independently
choose a root from one concurrent candidate and a tail from another. Today this
is achieved by the existing whole-column LWW behavior; adapters may later
replace that decision only through `ContentManifestAdapter::merge`, which is
passed full manifests.

### 19.2 Immutable identities and storage

`ContentId` is BLAKE3 over the fixed `jazz-content-id-v1` domain separator,
immutable kind (`leaf`, `node`, or `root`), authorization/encryption domain,
adapter kind, and canonical payload length and bytes.

`INV-CONTENT-2`: Every semantically relevant immutable field, including
adapter/version and domain, MUST be in the canonical payload or fixed preimage.
The authorization/encryption domain MUST be included, so equality does not
leak across domains.

`INV-CONTENT-3`: Immutable storage MUST implement
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

`ContentManifestAdapterRegistry` resolves the schema's `adapter_kind` to one
process-local adapter. Registrations are append-only for the process lifetime:
an adapter is registered during startup before a node can accept schemas that
name it, and attempting to replace an already registered kind fails. This
prevents two worker threads from interpreting a replicated manifest differently.
The ordinary row codec resolves and validates every tail operation through that
registry, so an unknown kind or invalid typed tail fails closed at admission.

`ContentManifestRuntime` is the narrow execution bridge for materialization,
adapter-defined merges, and interior projection/index derivation. It is given a
domain-scoped immutable store and always decodes the complete cell before
calling the adapter. Query/index code must use this bridge (rather than reading
the record fields or root directly). A concrete content lane may opt into adapter
merge only once it has the necessary immutable store and conflict semantics;
until then, core keeps normal whole-cell LWW.

`INV-CONTENT-4`: Any merge strategy, interior query, or index that observes
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
- Physical persistence/lifecycle of immutable content objects in a production
  node (beyond the adapter-neutral store contract) belongs to the first content
  lane that can supply it.

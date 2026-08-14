# 12. Text content adapter

## 12.1 Scope and foundation boundary

This chapter specifies only the text adapter for the ordinary content-manifest
foundation. The foundation owns the atomic-column contract, materializer
registration, merge-strategy dispatch, and interior-query/index integration.
This adapter supplies typed text roots, validates and materializes its own
bounded tail, and produces the adapter value consumed by those foundation
hooks. It MUST NOT introduce a text-specific mutable head or version table.

An application column declared as `content.text()` holds one atomic manifest:

```text
TextManifest {
  root: TextRootId
  editTail: TextEdit[]
}
```

The column's schema establishes that the manifest is text; the stored value
does not repeat a runtime type tag. A version of the owning application row
therefore names one exact text snapshot. Copying the manifest preserves that
snapshot; following an application-row reference follows later row versions.

`TextRootId` is distinct from arbitrary row IDs at the public TypeScript
boundary. It identifies an immutable rope node/root row accepted by this
adapter; it does not grant authority to read that row.

## 12.2 Immutable rope rows

The adapter stores a UTF-8 rope in immutable ordinary rows. A leaf contains
bounded UTF-8 text and its Unicode scalar (code-point) length. A branch
contains its ordered child IDs, the children’s subtree scalar lengths, and the
height needed to validate balancing. The root is a complete snapshot: readers
MUST NOT replay older manifests or Jazz row history to reconstruct it.

Leaf and branch IDs are content-derived from a domain-scoped canonical encoding
that includes the format version, node kind, all semantic metadata, and the
full leaf bytes or child identity/length tuple. The domain/encryption scope is
part of that encoding, so equal plaintext in different authority domains does
not reveal equality through a shared ID. An immutable-row insert is
idempotent only when an existing same-ID row has identical canonical contents;
a differing row is a corruption/error, never an overwrite.

## 12.3 Edit tail and Unicode contract

The tail is a bounded, ordered list of insertion intents:

```text
TextEdit { atCodePoint: u64, textUtf8: string }
```

Each `atCodePoint` is evaluated against the text after all earlier tail edits.
An edit MUST use an integer scalar offset in `0..=currentScalarLength`; adapters
MUST reject invalid offsets before authoring a transaction. Splitting a UTF-8
code unit or Unicode scalar is impossible because both validation and rope
navigation use scalar offsets, not JavaScript UTF-16 indices or byte offsets.

The persisted text format has hard limits of at most 64 edits and 16 KiB of
canonical UTF-8 tail encoding. Every reader validates encoded byte length
before parsing, validates count before materialization, and rejects malformed
or semantically invalid entries. Local writer limits may be lower but never
higher. An accepted writer may return an empty/no-op edit without rewriting
the owning column.

## 12.4 Materialization, merges, and interior access

`materialize(TextManifest)` loads exactly the addressed rope snapshot, applies
the ordered bounded tail, validates the resulting scalar length, and returns a
text value plus enough stable provenance for the foundation to invoke text
merge strategies and interior-query/index adapters. Missing, malformed, or
out-of-domain nodes fail closed; there is no history fallback.

Foundation merge strategies and query/index providers receive the materialized
text value (or a requested text range when their declared operation permits a
range). They MUST NOT compare only `root` and MUST NOT ignore `editTail`.
Consequently a `contains`, equality, order, or text-specific index operation
observes the same logical text as an ordinary application read.

The initial implementation need only expose whole-value materialization and
explicit range reads. Interior index pushdown may progressively use rope
subtree lengths, but every optimized result MUST equal materializing the same
manifest and applying its tail first.

## 12.5 Writes and promotion

A small insertion changes only the owning application row's atomic manifest:

```text
{ root: R7, editTail: [e1] }
  -> { root: R7, editTail: [e1, e2] }
```

When appending the proposed edit would exceed either writer bound, the adapter
synchronously materializes the current manifest, path-copies the affected rope
path (or builds a smaller complete balanced tree if scattered edits make that
cheaper), inserts only the reachable new immutable rows, and writes:

```text
{ root: R8, editTail: [] }
```

The immutable rows and owning-row manifest update belong to one ordinary Jazz
transaction. A denied owning-row update rolls back every newly inserted
immutable row. Untouched subtrees are reused by ID. A direct historical read
of a manifest continues to use its original root plus original tail after a
later promotion.

## 12.6 Required black-box evidence

- public `content.text()` schema declaration stores and reads a manifest as one
  atomic content column, with no adapter-created head/version row;
- inserting before, inside, and after non-ASCII scalars (including astral
  scalars and combining sequences) never splits a scalar and reports scalar,
  not UTF-16, offsets;
- a tail-only edit changes the owning row manifest while reusing its root;
- count and canonical-byte threshold crossings promote synchronously, clear
  the tail, and retain unchanged subtree IDs where a localized path is cheaper;
- a copied historical manifest reads exactly its old text after later edits and
  promotions; deleting its root makes that read fail rather than replaying
  history;
- malformed/oversized replicated tails and inconsistent rope metadata fail
  before observable content, and cannot poison an unrelated row update;
- a permission denial rolls back new immutable rows and the owner manifest as
  one transaction;
- identical immutable canonical rows are idempotently accepted, while a
  deliberately same-ID/different-content insert fails closed;
- equality/contains/range access and any enabled text index observe root plus
  tail, including an unpromoted edit; and
- a competing update is passed to the registered materializing merge strategy
  with both full logical values, never two unexamined manifest records.

## 12.7 Open implementation dependencies

The foundation PR must settle the exact public interfaces for atomic content
columns, manifest codecs, content-row insertion, merge-provider inputs, and
interior-query/index providers. The text PR will bind this contract to those
interfaces after it is stacked. It must not recreate those mechanisms locally.

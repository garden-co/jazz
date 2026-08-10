# Whole-row variants

Status: implemented on the zero-user new-core branch.

## Representation

Groove has one enum encoding rule for a scalar enum value and an extensible
whole-row variant:

- `EnumSchema` stores a declaration-order `u8` discriminant in a column;
- `TableSchema::variants` stores one ordered dense descriptor for each
  whole-row variant and `VariantRecord` stores its `u32` tag followed by that
  descriptor's encoded payload;
- a nested `ValueType::Union` stores its case tag and case-local record payload
  using the same canonical bounded-varint tag codec.

Every enum/union occurrence has a durable registry identity derived from its
physical field path. `TableSchema::value_variant_registries` persists those
registries independently of the descriptors that reference them. Descriptors
carry a case snapshot for local byte decoding, but structural equality is not
registry identity: live schema evolution reconciles snapshots by registry ID
and accepts only append-only prefixes. The hidden whole-row registry has its
own `TableSchema::variant_registry_id` and is never combined with nested
registries.

Consequently, adding a case to column A neither changes column B's registry nor
multiplies whole-row cases. Jazz allocates exactly one hidden row case per
physical schema layout; it has no `(layout × nested cases)` or public top-level
enum lowering.

The outer row tag is canonical u32 varint encoding. Tags 0 through 127 take
one byte; overflow and noncanonical encodings are rejected. Case declaration
is append-only: an existing tag, descriptor, and shared key identity are
immutable. Adding a new tag does not alter the bytes or interpretation of old
rows.

## Projection law

`VariantProjection` is the sole normalizing boundary for heterogeneous table
sources. It has an append-only case registry keyed by the physical variant tag.
Each case either projects its dense row payload to a fixed output descriptor,
constructs a nested `Union` value, or is explicitly ignored. An absent case is
an error, never silent filtering. `GraphBuilder::variant_project` subsequently
projects one named nested enum case and emits only that case's payload.

Thus the same variant machinery applies recursively: a top-level physical row
variant is normalized at a table source, while a variant-valued column is
selected by the graph operator. Neither needs a second row storage format or a
second query engine.

## Jazz lowering boundary

Jazz schema versions remain Jazz catalogue identities. They are not Groove
schema versions. For each physical table lineage, Jazz allocates an opaque
`u32` Groove variant tag for every retained dense layout and persists the
mapping atomically with its schema/lens metadata. Jazz writes and reopens
`VariantRecord`s; its source projections normalize them to the ordinary Jazz
row descriptor before query, policy, or lens evaluation.

Jazz does not expose top-level enum rows. Its public row model, wire values,
lenses, and schema APIs stay relational. Per-column enums are a separate Jazz
feature layered over Groove `EnumSchema`.

## Invariants

- Existing tag-to-payload meanings never change; adding cases is compatible.
- Every persisted tag has exactly one registered descriptor on reopen.
- A projection must register every observed source tag explicitly.
- Projection output identity remains fixed while cases are appended, so live
  subscriptions and prepared graphs are not rebuilt merely because a case is
  added.
- Primary keys and durable indices use explicitly shared physical identities;
  case-local same-name fields may have different types.
- Jazz lens reads/writes operate after source normalization and therefore do
  not expose a physical variant tag or a whole-row enum to callers.

## Coverage

`variant_tables` covers canonical encoding, additive variants, nested union
projection, same-name/different-type payloads, indexing, and reopen behavior.
`versioned_rows` (retained filename) covers mixed-variant replacement,
projection, index, and reopen behavior. Jazz catalogue-lens regression
`physical_schema_variants_survive_pointer_changes_and_reopen` covers
multi-schema read/write/reopen lowering.

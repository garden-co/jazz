# Generic variant tables: A/B implementation comparison

Status: local comparison prototype; not a compatibility commitment.

## Decision being tested

Both candidates retain dense descriptor-coded payloads and select the payload
descriptor with a table-local tag. They differ in which layer owns the
abstraction.

- **A — schema-layout versions:** Groove exposes `TableSchemaVersion` and
  `VersionedRecord`. Jazz uses them to retain old physical layouts and installs
  `VariantProject` cases at its logical-schema boundary.
- **B — generic open top-level union:** Groove exposes `TableVariant` and
  `VariantRecord`. Direct Groove users may use cases as domain variants. Jazz
  allocates cases for its storage layouts and immediately projects them to a
  homogeneous logical record.

This prototype steelmans B by changing the real storage codec and public
Groove schema/write/projection APIs, while retaining compatibility aliases so
the existing Jazz code remains runnable during comparison.

## Physical representation

B stores:

```text
[canonical u32 varint table-local case tag][dense case payload]
```

Tags 0–127 cost one byte, 128–16,383 cost two, and the maximum costs five.
Overlong and overflowing encodings are rejected. The current A implementation
stores an eight-byte little-endian `u64`, so B saves seven bytes per row for the
expected low-hundreds case count. The payload bytes are identical.

The bounded `u32` space is intentional. Exhausting four billion retained cases
would already make case registries and projections unusable. Jazz's durable
alias remains `u64`; lowering fails if it cannot fit the Groove-local tag.

## User union nested inside Jazz schema evolution

Do **not** serialize two nested tags. Groove has one flat, opaque physical case
namespace. Jazz retains this catalogue mapping:

```text
physical case tag -> (Jazz schema/layout identity, optional user-union case)
```

For two layouts and two user cases, Jazz may allocate four tags:

```text
1 -> (layout 1, text)       2 -> (layout 1, image)
3 -> (layout 2, text)       4 -> (layout 2, image)
```

This prevents tag collision and semantic conflation while retaining a one-byte
row prefix. It also handles a schema version which adds or removes a user case:
only realizable pairs receive physical cases. Projection cases may map multiple
physical tags to the same public user discriminant.

The cost is product growth in the worst case: `layouts × user cases`. This is
not extra row overhead, but it is registry and projection metadata. It should be
measured with real schema histories before committing to user-facing unions.

## Whole-path semantics

The prototype exercises public APIs for:

- declaring generic cases with different dense descriptors;
- writing `VariantRecord`s;
- immediately projecting four heterogeneous physical cases to one fixed
  descriptor, including a literal public user discriminant;
- maintaining a durable index over a field shared by all cases;
- replacing the fixed eight-byte storage header with canonical varints;
- reopening the database and restoring case/projection registries;
- adding cases without changing prepared graph identity.

Prepared source identity remains `(table, projection target)`, not the case
set. Case registration is append-only runtime metadata, so a live case does not
rebuild prepared graphs or reset subscriptions. As in A, durable rows require
their case descriptor and projection registrations to be restored before read.

### Index semantics

Existing variant-aware index behavior generalizes cleanly:

- an index over fields present in a case registers a projection for that case;
- a case missing any indexed field is ignored;
- uniqueness spans all participating cases;
- live case registration installs its index case without adding a new index
  source;
- reopen reconstructs the cases from the table schema.

One genuine B-specific gap remains: `IndexSchema` cannot directly index the
user discriminant because it names payload fields, while the discriminant is
the physical case tag. A production B API should allow an index projection to
include the normalized case literal (or expose a typed case expression). It
should not redundantly store the user tag inside every dense payload merely to
reuse the existing field-only index declaration.

## Implementation and complexity receipt

Compared with A's base commit, the prototype changes the abstraction rather
than adding a second engine:

- the runtime projection and variant-aware index implementations are reused;
- schema registry terminology becomes generic (`variants`, `tag`);
- schema-version APIs remain temporary compatibility aliases;
- Jazz's physical lowering calls the generic registration seam;
- the row codec changes to canonical bounded varints;
- a black-box integration fixture covers `2 layouts × 2 user cases`.

The resulting diff is approximately 300 changed lines plus the integration
fixture and this memo. Most changes are compatibility spelling and tests; there
is no second scan, IVM, index, or prepared-query implementation.

## Measurements

Debug-build manual receipt on this host, 20,000 mixed rows across four cases:

```text
commit + index maintenance + IVM projection + delivery: 323 ms
cold reopen + projection hydration:                         67 ms
case prefix:                                             1 byte/row
```

These timings are diagnostic, not a regression threshold.

A and B use the same descriptor lookup, projection, index, and downstream IVM
code. Therefore the only hot-path implementation difference in this prototype
is parsing a 1–5 byte varint instead of loading eight fixed bytes; whole-path
timings are expected to be noise-equivalent. B materially improves stored and
in-memory retained row bytes by seven bytes per row for tags below 128. Registry
memory is unchanged per physical case; nested user unions increase the number
of cases as described above.

## Recommendation

Prefer **B**, but describe it precisely as an _open table-level variant source
with immediate normalization_, not as recursively union-aware IVM.

The implementation evidence says A's machinery was already almost the generic
primitive: its storage lookup, append-only case registration, fixed-output
projection, live index maintenance, reopen behavior, and prepared identity all
transfer directly. Keeping schema-version language in Groove buys no simpler
engine and prevents direct users from using an otherwise generally useful
feature.

Before productionizing B:

1. design user-facing discriminant index/filter expressions;
2. make Jazz's `(layout, user case) -> physical tag` mapping durable and atomic
   with case descriptors;
3. benchmark registry memory for realistic layout/user-case histories;
4. decide whether old fixed-u64 rows need an explicit format marker/migration
   path (the zero-user new-core branch currently permits no migration);
5. finish removing compatibility schema-version names from Groove once Jazz
   lowering is converted.

## Verification receipt

- `cargo test -p groove --no-fail-fast`: green (406 library tests passed, one
  receipt ignored; every integration and doc-test target passed).
- `cargo check -p jazz`: green.
- `measure_variant_write_projection_and_index_path -- --ignored --nocapture`:
  green with the measurements above.
- The focused Jazz physical-reopen test did not reach execution: compiling all
  Jazz integration targets exhausted the root filesystem while linking. This
  was a host resource failure (`No space left on device`), not a test failure;
  the prototype target directory was subsequently cleaned.

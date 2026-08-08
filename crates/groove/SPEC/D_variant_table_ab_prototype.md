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

The mapping is now part of Jazz's serialized `SchemaPhysicalMapping`, the same
payload written atomically with schema admission. Reopen validates uniqueness
across every layout sharing a physical table. Allocation preserves existing
case definitions, rejects mutation, and advances beyond all retained tags.

### Case-local names versus shared physical identities

A global field-name catalogue is not strong enough for a user union: two cases
may both call a field `value` while one stores text and another stores an
integer. B therefore gives every case a local payload descriptor. A case field
only participates in cross-case primary keys or indices when it explicitly
binds a table-wide shared physical identity of the same type.

The prototype proves both layers:

- Groove stores and reopens `text.value: String` and `metric.value: U64` in one
  table while both cases explicitly bind differently named local id fields to
  the shared `id: U64` identity and index;
- Jazz durably serializes those case-local descriptors and shared physical
  column ids, reopens them, and lowers the two cases to distinct dense Groove
  descriptors without conflating the two `value` fields.

## Whole-path semantics

The prototype exercises public APIs for:

- declaring generic cases with different dense descriptors;
- writing `VariantRecord`s;
- immediately projecting four heterogeneous physical cases to one fixed
  descriptor, including a literal public user discriminant;
- maintaining a durable index over a field shared by all cases;
- allowing identical case-local names to have different types while explicit
  shared identities retain cross-case key/index semantics;
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
- Jazz's physical mapping durably carries case tags, local descriptors, and
  optional shared physical column ids; allocation and reopen reject collisions;
- Jazz's physical table lowering emits each stored case descriptor and its
  immediate fixed-output projection, filling absent union fields with typed
  nulls;
- the row codec changes to canonical bounded varints;
- a black-box integration fixture covers `2 layouts × 2 user cases`.

The resulting exploratory diff is about 1,803 added and 206 removed lines,
including fixtures and this memo. The size is no longer merely terminology:
roughly half is Jazz's durable allocation/lowering seam and adversarial tests.
There is still no second scan, IVM, index, or prepared-query implementation.

## Measurements

The identical compatibility-API benchmark was run in release mode on A's base
commit and B, seven fresh-database repetitions each. It writes 20,000 mixed
rows across four dense cases, maintains a shared-field durable index, projects
to a fixed output, consumes delivery, reopens, restores cases, and cold-hydrates
all rows. The first repetition is reported but excluded as warm-up.

```text
                     A fixed-u64                  B varint
commit+index+IVM:     median 58.296 ms             median 58.342 ms
                     range 57.557–59.729 ms       range 57.614–58.993 ms
cold reopen/scan:     median 10.107 ms             median 9.809 ms
                     range 10.009–10.286 ms       range 9.761–9.909 ms
persisted tag bytes:  8                            1 for tags 1–4
```

The commit medians differ by 0.08%; B's scan median was 2.95% lower. At this
sample size both support the narrower conclusion that varint parsing is not
material to whole-path cost. These timings are diagnostic, not thresholds.

A and B use the same descriptor lookup, projection, index, and downstream IVM
code. Therefore the only hot-path implementation difference in this prototype
is parsing a 1–5 byte varint instead of loading eight fixed bytes. B saves seven
bytes in persisted encoded row values for tags below 128. It does **not** save
seven bytes in decoded runtime state: runtime deltas already carry the tag as a
machine integer separate from payload bytes, and retained projected rows no
longer carry the source prefix. Registry memory grows with physical case count;
nested user unions increase it as described above.

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
2. expose Jazz's implemented internal case mapping through a public union
   schema/write API and carry the selected user case through replication;
3. measure registry/runtime memory for realistic layout/user-case histories;
4. decide whether old fixed-u64 rows need an explicit format marker/migration
   path (the zero-user new-core branch currently permits no migration);
5. finish removing compatibility schema-version names from Groove once Jazz
   lowering is converted.

## Verification receipt

- `cargo test -p groove --no-fail-fast`: green (406 library tests passed, one
  receipt ignored; every integration and doc-test target passed).
- `cargo check -p jazz`: green.
- `cargo test -p jazz variant_case_tests --lib --no-fail-fast`: 3 passed,
  including durable evolution/reopen, collision rejection, and same-name
  different-type Jazz-to-Groove lowering.
- `cargo test -p groove --test variant_tables --no-fail-fast`: 3 passed, one
  manual receipt ignored.
- `repeated_release_write_ivm_and_cold_scan_receipt`: green on both A and B,
  seven repetitions each, with raw samples retained in the lane report.
- `measure_variant_write_projection_and_index_path -- --ignored --nocapture`:
  green as a separate debug-build smoke receipt.

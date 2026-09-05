# Typed identity durable role receipts

The governing all-in format is Jazz SPEC 16, “Typed identity descriptor roles.”
Compatibility with the retired contained experiment is not a goal. This record
separates actual durable byte changes from changes to diagnostic test rendering.

## Explicit codecs and identity ownership

- `protocol::version_record_wire_row` owns the `JVRR` v1 row blob. The outer
  immutable receipt keeps schema UUID, logical table, branch, and authored
  column identity. Its nested descriptor never serializes compiler slots.
- `node::descriptor_roles` owns `JRPD` v1 current and aggregate role schemas.
  Readers compare complete canonical role/name/type trees against the compiled
  schema before installing runtime identities, and compare row/group/replacement
  values with their member identity. Equal-width type substitutions are rejected.
- `terminal_root_layout` hashes the new `jazz terminal root publication v1`
  domain. Public names and source/result/provenance roles affect the hash;
  compiler allocations and node-local physical-column aliases do not.
- Group and aggregate aliases can both be `count`: independent role ordinals
  retain both values. A reader with different compiler slots or local column
  IDs rebinds only after canonical schema equality. Schema evolution that changes
  a logical name or value type requires the matching compiled schema; it cannot
  reinterpret a previous payload merely because its row width is unchanged.

The codec tests pin these deterministic BLAKE3 fixtures:

| Fixture                                      | Hash                                                               |
| -------------------------------------------- | ------------------------------------------------------------------ |
| `JVRR` nested immutable row                  | `49f95ea224a6eb504d45a80ec11f003fa4717998d4d477198716237c865d0875` |
| `JRPD` duplicate group/value name descriptor | `89fb1b80e90645fc68aaeccf9fbcd4082bcefdd2b606c8ed859827bd99465fff` |
| Enclosing aggregate `JPFK` payload fixture   | `612a52116020e1219c5b75f67dd18d491485a9b1579a06cadf7d16c18a84631a` |
| Root publication layout fixture              | `0d6d813bb94f2ded6db0616b3a2d55d7e9c8bd8173ebc6395f766a18ca7d114f` |

The aggregate row fixture is exactly
`01000000000000000200000000000000`: two declared little-endian U64 values,
independently selected through their compiled identities. Unknown role/version,
trailing bytes, changed logical identity, changed role, and same-width changed
type are rejected. Recursive descriptor framing and its 1024-node bound are
owned by Groove's persisted-descriptor grammar, not a parallel Jazz type codec.

## Native producer pack: diagnostic serde change, identical policy-store bytes

The current-producer pack was introduced by `ef66e8ffd3`. The later
`1077bafaa3` added `DescriptorField.identity` and default logical identities.
`native_corpus_receipt` retains raw table bytes, but renders **decoded direct-store
Values with postcard**. That rendering includes nested `OwnedRecord`
descriptors in `jazz_authority_policy_bindings`. It is a diagnostic semantic
fixture, not the raw direct-store format.

The producer under compiler checkpoint `2d666d2237` independently reproduced the
new pack before this durable-codec change. A reversible diagnostic mutation that
skipped only `DescriptorField.identity` in serde reproduced the previous pack
exactly. The mutation was restored before normal tests; it is not production
compatibility code.

| Producer rendering        | Complete pack SHA256                                               | Policy semantic value bytes |
| ------------------------- | ------------------------------------------------------------------ | --------------------------- |
| Previous descriptor serde | `cd2eed57320d8d18bd99b2be552fb7de1ac4e35588c63dcb729fc2915de9105a` | 490                         |
| Typed descriptor serde    | `2acca4b24d4d4128f7d18e13c14df973a93cb0aa3d65a9287bc9eb6543b584b3` | 619                         |

Only the policy-directory semantic entry differs. Its key and interpreted
subject/claims remain the same. Both exported SQLite candidates contain exactly
one policy-directory raw entry in `__groove_class_meta`; its key and value are
byte-identical across the diagnostic mutation:

- Physical key: 70 bytes; SHA256
  `a275172acda1b552c0715e2d0c98bdff0aa366409d32dbd67df8bc780ded21e3`.
- Raw value: 75 bytes; SHA256
  `28a54fa95df3176f94b5a7d6da464a6e300226f15018fed64524bd363444f234`.

All other raw KV entries also agree except the backend-owned encrypted chunk
payload, whose physical encryption bytes are not this interchange receipt.
The direct-store writer still creates the declared Groove record and writes its
raw bytes; the extra descriptor identity metadata occurs only in test-pack
postcard rendering.

Generation uses the guarded existing producer path, including RocksDB and SQLite
reopen, mixed writes, fresh reopen, exact semantic assertions, backend equality,
and staged no-overwrite candidate publication:

```sh
ulimit -n 65536
RUST_MIN_STACK=4194304 \
JAZZ_NATIVE_CORPUS_PACK_OUT=/tmp/typed-current-producer.pack \
JAZZ_NATIVE_CORPUS_SQLITE_OUT=/tmp/typed-current-producer.sqlite \
cargo nextest run -p jazz -p groove --no-default-features \
  --features jazz/testing,jazz/transport-compression-zstd --lib \
  -E 'test(settlement_baseline_native_jazz_corpus_reopens_and_accepts_mixed_writes)' \
  --no-fail-fast
```

Output paths must not exist. The export variable skips the old diagnostic golden
comparison, not candidate validation. The current producer fixture and its
checksums are updated; committed historical epoch packs and binary native
fixtures remain unchanged and retain their independent historical-open tests.
The native corpus does not by itself prove every result-payload role: those
boundaries have separate strict codec and maintained/query differential tests.

## Test sensitivity

A reversible mutation bypassed only the canonical role-schema equality checks
in both current and aggregate readers. Both role canaries then failed on the
specific assertion that a changed logical name must be rejected (0/2 passed).
The mutation was restored exactly before the final canonical suite. This proves
the tests detect positional reinterpretation rather than only checking goldens.

Final implementation validation used the canonical 4 MiB Rust test stack and
Jazz testing/zstd feature selection: **2503 tests passed, 4 skipped** across Jazz
and Groove library targets. Workspace `cargo check --workspace --all-targets
--features jazz/testing,jazz/transport-compression-zstd` also passed. These are
Rust receipts, not a claim that the complete local CI-equivalent workflow ran.

## Host input and recovered receiver follow-up

The explicit named-cell input role in SPEC 18 is shared by NAPI and WASM.
A literal input fixture and nested Array<Record> case reject execution-descriptor
serde; restoring that old generic decoder makes the literal test fail. Additional
cases exercise duplicate/absent names, unknown tags, bounded depth/node count,
and payload-enum input. The exact local write-merge row reader binds its source
publication identities through the requested schema before the host encoder;
its real Db transaction canary failed with unresolved publication roles before
that correction.

The shifted-local-column-ID catalogue test now serializes and decodes its actual
immutable update through `JVRR` before applying it and reopening RocksDB. The
nested aggregate codec canary independently moves nested execution slots and
rejects changed nested names/types or a mismatched replacement identity.

`maintained_nested_and_aggregate_results_rebuild_from_persisted_receiver_without_authority`
uses a serialized authority closure, records complete nested results and grouped
counts, then drops both nodes. The receiver reopens RocksDB and compiles fresh
maintained graphs before comparing root/child identities, all nested values,
and aggregate group identities/values. No authority survives to provide a fresh
response. A fresh-empty-receiver mutation fails the full nested-value equality;
the persisted-receiver version passes. These complementary receipts cover real
wire/local-ID translation, exact nested slot rebinding, and cold receiver
reconstruction without claiming that one test combines every topology.

## Current wire and host-fixture receipts

The current `wire_message_frames.json` manifest retains exactly 25 families.
Only `authority_publication_two_complete_transactions` (frame 918 → 1998 bytes)
and `view_update_mixed_version_carrier_runs` (1820 → 3980 bytes) change: their
immutable row carriers now use `JVRR` plus explicit persisted descriptors.
The owning fixture test recreates the same semantic messages, verifies current
writer bytes, decodes all manifest messages to their expected values, and runs
the host-frame rejection corpus. All 11 wire-fixture tests pass.
`native_row_codec.json` remains byte-identical; its Rust fixture producer now
uses the explicit shared binding descriptor encoder instead of recursive
execution-type serde.

Two TypeScript mock producers now explicitly declare source output names and
provenance/hidden roles while retaining their private `user_*` carrier spellings.
Their behavioral assertions are unchanged; the input fixtures no longer assume
that a consumer reconstructs public identity by stripping a carrier prefix.

## Standalone integration target audit

The `--lib` receipts above exclude integration binaries. A separate canonical
4 MiB run of every Jazz/Groove integration binary measured 284 passes and two
failures at the typed checkpoint: `large_json_wire::json_version_records_freeze_inline_and_indirect_semantics`
and `branch_views::branch_view_reduction_precedes_aggregation_and_ordered_windows`.
The repaired batch passes all 286 tests, with four skipped. Reproduce this
coverage with `RUST_MIN_STACK=4194304 cargo nextest run -p jazz -p groove
--no-default-features --features jazz/testing,jazz/transport-compression-zstd
--tests -E 'kind(test)' --profile jazz-ci --no-fail-fast`; library-only commands
do not substitute for this receipt. The official workspace partition includes
both library and integration targets.

The branch-view relation snapshot materializer had interpreted synthetic
aggregate output as a source-table row, then attempted to resolve the count
alias as a catalogue column. It now uses the compiler's full aggregate app-row
schema and the shared aggregate conversion, retaining result identities,
publication names, ordering and windows. The existing black-box branch-view
count/window assertions fail before this change and pass afterwards.

The standalone `large_json_wire_v1.json` corpus was last produced before `JVRR`.
Its owning test now supports the existing `JAZZ_UPDATE_WIRE_FIXTURES=1` writer
convention and recreates the same inline JSON, indirect large-JSON reference,
and deliberately wrong String-descriptor example through `VersionRecord`.
The last example remains a malformed semantic kind inside the current envelope,
not an old-format reader or a compatibility path. Inline bytes grow 357 → 895,
indirect 474 → 1012, and wrong-String 355 → 895. Each row envelope contains a
680-byte explicit descriptor. The canonical raw row and outer authored-column
suffix are byte-identical to the old corpus: inline/wrong-String raw rows are
175 bytes (SHA-256 `f42a2be2b181f3d07e0b6ff48dc2b699edf5453a9592fe38263ee7157e3541cc`),
and indirect raw rows are 292 bytes (SHA-256
`29bd9bed0e0b4269adc0d2ab73ec6e8a5e99116f6e602bf15234ff3964442d8b`).
The test retains inline/indirect semantic-kind and raw-byte assertions, exact
round trips, the wrong-descriptor rejection receipt, and an explicit `JVRR` tag
assertion. Other standalone integration targets contain no additional embedded
row-byte corpus beyond the already-covered `wire_fixtures` target; the persistent
codec registry checks exact proof anchors rather than encoding records itself.

## Reviewed React Native integration

The full reviewed RN ancestry at `0a788192db` merges into the typed branch.
Typed application-storage names and current `JVRR` fixture bytes remain
authoritative; both independently added read tests are retained. The merge
keeps the explicit NAPI/WASM named-cell readers and exact local-row schema
publication binder while bringing the reviewed upstream admission, cancellation,
detach, committed-deletion, and permission-advice behavior together.

An initial combined core/native Rust run measured 2972 passes and ten failures.
Every failure came from the relay test consumer reading the new publication
descriptor as the generic execution `RecordDescriptor`. The test consumer now
reads the explicit stored/result/hidden role grammar and recursive name/type
grammar; exact content, identity, lifecycle and real reopen assertions remain.
The sibling RN production cell reader also still used generic descriptor serde,
which its old fixture producer masked. It now delegates to the shared named-cell
reader, and fixture producers use the shared named-cell encoder. All 83 relay
tests pass. A literal U64 ABI input at the RN boundary passes with the shared
reader; a planted generic descriptor read fails with `DeserializeBadOption`,
then is restored before the combined gate.
The restored combined Jazz/Groove/NAPI/native-relay/native-transport library and
integration run passes all 2982 tests, with eight skipped, using the canonical
4 MiB stack and the RN bridge feature. This is still a Rust receipt; default
and RN bridge artifact consumers remain separate required gates.

This inventory is scoped to descriptor ownership, not a repository-wide claim
that Postcard is runtime-only. Outer peer transport still uses its frozen
Postcard grammar. Query and binding IDs use explicit canonical request bytes;
the policy-branch semantic label and authorization-support digests still hash
Postcard tuples. Their indirect use in durable identities requires a separate
freeze audit before claiming that all durable hash inputs are independent of
serializer layouts.

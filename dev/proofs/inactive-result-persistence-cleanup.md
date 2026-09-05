# Inactive result persistence cleanup

Issue #2578 removes the former persisted-output model before the storage freeze.
The receiver still persists authority-approved source closure facts and derives
queries locally. For example, a receiver reopens its stored source versions and
rebuilds nested rows and grouped counts without receiving another authority
response. An authority-supplied aggregate payload or membership delta still
rejects before any source or query-state mutation.

## Production call-path proof

`views::validate_view_update_payloads` rejects member additions/removals and all
facts except `ProgramSourceCoverage` and `CoveredInput`. The only production
caller of the former settled-result persistence wrapper is the admitted view
update path. That path can never populate the member store or persist any of
the other thirteen runtime fact variants. Recovery nevertheless registered and
read those unused formats. The cleanup removes that mismatch: the renamed
`persist_source_closure_delta_for_authority_result` writes only source facts;
its codec rejects every other variant at both writer and reader boundaries.
The encoder's production callers are only `settled_program_fact_key` and
`settled_program_fact_storage_write`; the decoder's only production caller is
`recover_known_state_facts`. Runtime fact ordering, hashing, and publication do
not call this durable codec.

The retired authority-member indexes have no remaining producer. The separate
binding-view member cache was never populated by production at all; only tests
injected fake cache entries. Those fields, unused mutators, and the vacuous
uniqueness helper are removed. The tests retain their observable canonical-row,
schema projection, policy-tier, and catalogue replay checks. The remaining
unwritten binding-view fact/progress/hydration shadows are also removed; their
corruption tests now inspect active `AuthorityResultState` receipts. The
write-only `known_state_declared_binding_views` shadow had no reader and is
removed; the authority-scoped known-state field remains active.

## Before and after inventory

| Family or representation                               | Before                                                                  | After and concrete purpose                                                                                                                                                      |
| ------------------------------------------------------ | ----------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Local source history, deletion, registers, and indexes | Active storage                                                          | Unchanged Groove records under catalogue-owned physical layouts. No JVRR envelope or per-row descriptor is stored here.                                                         |
| Network `VersionRecord` / `JVRR`                       | Active wire format                                                      | Unchanged descriptor/row envelope validated before translation into local physical storage.                                                                                     |
| `jazz_settled_program_facts` / `JPFK` v1               | Two active source variants plus thirteen inactive output/proof variants | Only dense tag `0` source coverage and tag `1` covered input. `views` admission → durable writer → `recover_known_state_facts` → covered-input indexes → local IVM.             |
| `jazz_known_state_facts`                               | Active closure progress                                                 | Unchanged. Exact authority identity, settlement/authorization progress, and source-closure generation support reopen admission.                                                 |
| `jazz_authority_policy_bindings`                       | Active exact policy directory                                           | Unchanged. Bounded digest addresses exact subject/claims; collision/mismatch checks remain before write and recovery.                                                           |
| Closed required-codec profile                          | Advertised the two dormant result codecs                                | Rust and IndexedDB remove both IDs. JSM1 required-family count changes from 14 to 12; existing roots advertising the retired profile reject.                                    |
| `jazz_settled_result_members`                          | Registered but never populated by admitted production updates           | Removed with writer, digest domain, reader, and schema declaration.                                                                                                             |
| `JRME` / `JRSE`                                        | Dormant member/source storage encodings                                 | Removed, including storage-only record declarations, nested parsers, and obsolete golden fixtures.                                                                              |
| `JPFK` result payload/member/proof variants            | Dormant encodings                                                       | Removed. Their old tags reject; no reinterpretation or compatibility branch.                                                                                                    |
| `JRPD` Current/Aggregate                               | Shared runtime publication and dormant persistence representation       | Runtime only. Local materializers encode, schema-bound readers compare exact role/type trees and validate member/occurrence identity. Generic storage recovery decoder removed. |
| Synthetic aggregate row/replacement bytes              | Runtime identity plus dormant member storage                            | Runtime only, byte-identical. `runtime_result_identity_bytes` serves maintained and one-shot aggregate execution and JRPD identity checks.                                      |
| Root publication / flat-tuple revision hashes          | Runtime identity                                                        | Unchanged. Needed for terminal publication and occurrence replacement semantics.                                                                                                |
| Protocol `ResultMemberEntry` / `ProgramFactEntry`      | Runtime execution/wire types                                            | Retained. Deleting unused persistence does not delete local result values or change native wire enums.                                                                          |

## Retained byte contracts

Jazz SPEC 16 specifies the full `JPFK` field order, primitive widths, bounds,
source role tags, version references, and canonical rejection rules. The only active tags are dense `0` and `1`; the field payloads remain unchanged.
The direct-store key is the five-field authority prefix followed by the
32-byte BLAKE3 derived key in domain `jazz.settled-program-fact-key.v1`; the
value is one `Bytes` field holding the exact canonical fact. Retired tags are
not aliases for the remaining variants.

Runtime synthetic identities keep the fixed Groove record
`[descriptor: Bytes, value: Bytes]`; the first field is a canonical descriptor
for one `value` field and the second is its encoded value. This representation
has no production persistence caller. The `JRPD` role/name/type and ordered
occurrence checks remain intact, including rejection of reordered source roles,
changed joined UUIDs, and substituted union labels.

## Verification scope

Existing receiver RocksDB nested/count reopen tests with no authority response,
covered-input add/remove/reset/rewrite/reopen tests, and malformed closure
recovery tests remain acceptance gates. New internal byte tests reject every
retired fact tag, malformed booleans, wrong versions, truncation, and trailing
bytes. The store-absence test checks that source closure survives reopen while
the retired member store is not registered. These boundaries are internal
because public APIs cannot inject raw storage bytes or enumerate internal stores.

Historical corpus artifacts remain historical evidence. Any current-producer
inventory change must be explained by exact family/entry differences rather
than silently regenerating a failing baseline.

## Exact corpus inventory delta

The live SQLite/RocksDB producer matches the precomputed old-pack-minus-empty-
family prediction exactly. The sole removed line is
`store\tjazz_settled_result_members`; the JPFK v1 tag transition changes the
affected `jazz_settled_program_facts` direct-store keys and values while the
remaining producer inventory stays fixed. Direct-store entries in this pack render decoded Values;
this is not a claim that complete physical roots are byte-identical. Current pack SHA256 is `4aec397721f146845becdf0d5268a2229242a88ffe8b882312345d0039482d65`;
semantic receipt SHA256 is `180cb5a7e50253ff1880c4a066ac987de4c889acee49fd93bdb75782727b9699`.
Historical blobs/checksums remain unchanged. Their retired required-codec
profiles now fail real current adapter admission. No comparison normalization or
fake historical profile is used. New positive physical fixtures were exported
through the canonical live producer and reopened through current adapters.

## Closed-profile delta and current physical receipts

Deleting dormant codecs also removes `jazz.result-member-key.v1` and
`jazz.result-row-source.v1` from both Rust and IndexedDB required-family lists.
The JSM1 sample now has 12 required families and SHA256
`a3e89ed15b6b2b243fb15c3eef650d843398cf081ecf3be73f650e741349fe96`.
This manifest change is separate from the unchanged 35 logical source entries.
Old manifests reject as inconsistent with the current adapter, before ordinary
row interpretation; this is an intentional pre-freeze contract change.

Current native physical fixtures came from the guarded producer, including
independent staged-candidate reopen and source removal:

- SQLite gzip SHA256: `8ad336a716f3166896cd9d5bcbc73140871bb2b6ed1dbe1e06589db924eb39ef`.
- SQLite raw SHA256: `68392b7e23153baece369a7905532c5ed52efb1f572a35383cbfedaefa3e251c`.
- RocksDB archive SHA256: `d215f099b52e40da63ad1b8d9ea55efa44deb67fe6811f870b185fc63b4314d6`.

The browser producer uses real Chromium/public WasmDb, deployed catalogue,
branch history and large values. It closes the writer, snapshots raw IndexedDB,
reopens with the server blocked, validates both branches, then optionally exports
through `JAZZ_BROWSER_CORPUS_OUT` to a new path. The committed current browser fixture has SHA256
`c4973d707d6a241967ddc4a766b3f7d759747eda3c207b996a788c5c479f0b4e`;
it was generated by the combined V1/dense source closure.
Historical fixture rejection remains separate. An intermediate artifact build used to create this fixture is not a
final consumer receipt.

## Focused commands

These Node tests are textually included under the `harness` module; source file
paths are not compiled module paths. Exact public wrapper examples:

```sh
dev/t node::tests::harness::maintained_nested_and_aggregate_results_rebuild_from_persisted_receiver_without_authority
dev/t node::tests::harness::retired_result_codec_profiles_reject_historical_native_roots
dev/t node::tests::harness::corrupt_settled_program_fact_recovery_does_not_publish_a_valid_prefix
dev/t --test persistent_codec_family_registry authoritative_persistent_codec_family_registry_is_complete_and_current
```

The intermediate browser producer receipt passed 1/1 in real Chromium after an
official same-checkout NAPI/WASM build and admitted JazzTools build. Its manifest
and generated fingerprint patch were archived externally, then the three tracked
generated files were restored before committing the new fixture. The final
pinned browser consumer still requires a final-source official artifact receipt.

The source closure persistence wrapper no longer accepts an optional arbitrary
rewrite set: its only caller always passed `None`. Actual source replacement
remains the admitted reset (`cleared`) plus canonical additions, as exercised by
the existing add/remove/reset/reopen tests.

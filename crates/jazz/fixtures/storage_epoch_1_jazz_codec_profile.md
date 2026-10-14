# Jazz storage profile receipts

## Independent root profiles

The physical storage epoch remains 1. Each independently persisted root
requires exactly one Jazz-owned format marker in addition to Groove's base:
`groove.large-value.v1`, `groove.ordered-chunk-storage.v1`, and
`groove.ordered-kv.v1`.

| Root                | Jazz marker                 | Additional Groove family  |
| ------------------- | --------------------------- | ------------------------- |
| Client, relay, Core | `jazz.node-storage.v1`      | `groove.durable-index.v2` |
| Server catalogue    | `jazz.catalogue-storage.v1` | None                      |
| Account registry    | `jazz.account-storage.v1`   | None                      |

Node format v1 covers branch keys, catalogue state, linear row history,
transactions (including the boolean `is_partial`), durability, touched-row
lists, and all other node-owned records. Catalogue format v1 covers entry
keys, entry envelopes and persisted catalogue payloads. Account format v1
covers journal keys and command records. None inherits another Jazz profile.

Individual byte families keep their named receipts in
`persistent_codec_family_registry.json`; only entries with `profile` are
manifest members. This separates encoding documentation from root admission.

## Byte receipts

`storage_codec_profile::tests::root_profiles_have_pinned_manifest_bytes`
pins all three exact `JSM1` manifests (memory adapter version 1, no parameters):

- node: `86b6e92f62698e63dd7e8ef847f967759f1a3b1987a510c8973609d514662e14`
- catalogue: `9c276c4812eaba0e9017cf9d08d385d18561bd1c6a1f4014f33b085668d23bb9`
- account: `4590c8707554a760c39bfb9333e6b6856ec2c61b8547f1b63bc2094036d34b9f`

## Compatibility

Manifest admission compares complete inventories, not independently negotiated
features. Previous per-family Jazz profiles, missing/future formats, and roots
opened as the wrong kind are refused before ordinary data is decoded or
mutated. Older binaries likewise reject these consolidated profiles. No
migration, fallback reader, physical epoch bump, or wire change is introduced.

Historical published/epoch-one/pre-linear fixtures remain immutable refusal
evidence. Current native and browser fixtures are regenerated through their
production-adapter producers. Node, catalogue and account reopen tests cover
their independent current profiles. The browser's replica identity and lease
metadata remain independent physical metadata and are unchanged.

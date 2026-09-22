🤖

## Before

The first frozen server catalogue payload epoch used inconsistent outer labels:
schema `v12`, lens `v5`, and permissions `v2`. They were not compatibility
formats—decoders already accepted only their respective current byte—but the
labels made the first settled format look like a migration history.

## After

All storage-epoch-one nested catalogue envelopes encode and accept only outer
`v1`:

- schema;
- lens transform;
- permissions;
- permissions bundle; and
- permissions head.

The old `12`, `5`, and `2` labels have no aliases or decoder paths. Recovery
therefore fails closed before resident catalogue state is built when it sees
one of those bytes.

## Examples

An empty lens transform is now exactly:

```text
01 00 00 00 00 00 00 00 00
^v1 ^operation count  ^draft count
```

A schema payload and a permissions payload likewise begin with `01`. Exact
golden tests cover the nested JSON and relation-tree forms, while a rejection
test proves each old outer label is unsupported.

## Non-goals

This does not change `JCAT` entry `v1`, the nested JSON/policy algebra `v1`,
Groove/Jazz node codecs, or canonical runtime schema-generation identities.
Those are distinct version domains and remain unchanged.

## Verification

- focused catalogue codec goldens and legacy-label rejection;
- persistent server recovery rejection for a corrupted nested payload;
- persistent codec-family registry invariant; and
- clean diff/status checks.

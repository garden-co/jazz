# INV-API-32

- Status: now
- Coverage: ✓

## Invariant

`ReadOpts.tier` selects the sufficient materialized knowledge and first-result gate; `Propagation` only controls whether evaluation or coverage may be forwarded upstream and MUST NOT change local-tier result semantics. Thus a `Local` read resolves from current local materialized state even with `Propagation::Full`: a locally committed pending write is returned, while a row written remotely but not yet delivered locally is absent. `LocalOnly` prevents upstream routing; it is not what makes a `Local` read local.

## Enforced by (tests)

`packages/jazz-tools/tests/browser/db.disconnect.test.ts`; `jazz::tests::browser_relay_durability::browser_relay_does_not_publish_a_premature_settled_snapshot`

## Implementation

`packages/jazz-tools/src/runtime/db.ts::Db::all`; `packages/jazz-tools/src/runtime/db.ts::Db::subscribe`

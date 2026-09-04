# INV-SYNC-32

- Status: target
- Coverage: untested

## Invariant

A receiver MUST select history/current and branch-overlay winners in authored lineage before projection, decode every fact under its authored schema, project it through the ordered lineage into the binding read schema, and run local IVM to derive terminal output. It MUST NOT supplement that residual program from unrelated local history. Raw bytes MUST NOT be relabeled as another schema and peer sync MUST NOT introduce `ProjectedAppRow` or an equivalent carrier.

## Enforced by (tests)

NONE-FOUND

## Implementation

planned canonical witness-closure ingress, branch resolver, and local maintained IVM feed

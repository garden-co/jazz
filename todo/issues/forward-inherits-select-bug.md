# Forward INHERITS SELECT fails to expose child rows

## What

Forward `INHERITS VIA <fk>` select policies fail to expose child rows to sessions that should inherit access from the parent row.

## Where

`crates/jazz-tools/tests/policies_integration/inherited_policies.rs`

## Steps to reproduce

1. Create a parent row whose SELECT policy grants access to `alice`
2. Create a child row with `allowRead` defined as `INHERITS SELECT VIA <fk>`
3. Query the child table as `alice`

## Expected

`alice` sees the child row via the parent-granted inherited SELECT path.

## Actual

The child row stays hidden, and follow-on scenarios that depend on inherited child visibility fail or time out.

## Priority

high

## Notes

Observed while adding inherited integration coverage for:

- folder-backed child visibility
- multi-FK OR composition
- multi-hop forward inheritance
- parent-granted update/delete flows
- parent-change / child-retarget subscription invalidation

# Policy Errors Should Include Denial Reasons

## What

Policy-denied errors (e.g. `WriteError("policy denied INSERT on table todos")`) include
no information about why the policy rejected the operation, making them hard to debug.

## Priority

medium

## Notes

Server-side denial reasons may expose sensitive data and should not be propagated
to clients in production.

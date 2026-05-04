# Optimistic Update DX

## What

Developer-facing API for mutation settlement state — show pending/confirmed/rejected status on rows and filter queries by settlement tier.

## Notes

- Settlement tracking internals already exist (`PersistenceAck`), but there is no developer-facing way to surface pending or rejected states in UI or handle rollbacks explicitly.
- Main consumers are app developers building UIs that need sync status and permission-rejection handling.
- Already done: ReBAC policies, sync settlement tracking internals, and scoped backend clients.
- Remaining work: expose settlement state in the API, add query filters for confirmed vs pending data, communicate rejection reasons, and define offline-duration handling patterns.

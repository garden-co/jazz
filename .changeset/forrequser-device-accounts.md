---
"jazz-tools": patch
---

**`forReqUser` now acts as a device account.** When `authenticate` returns `acceptDeviceAccounts(request)` and the request carries the account's proof from `db.getDeviceAccountProof()` as `Authorization: Bearer`, `jazz.forReqUser(request)` returns a database scoped to that device account, under your permissions, as its own sync is. Before, it threw `signed_out` for every device account. Anyone can make a device account: if a route relied on `forReqUser` throwing to mean "a real user is signed in", check `accountFor(request)` (which still answers `null` for device accounts) or your own session instead.

`withAttribution` and `accountFor` keep answering as before for device accounts (`signed_out` and `null`) unless you pass `{ deviceAccounts: true }`. A proof is checked as a sync connection's is (signature, expiry, audience, and a key a link revoked), and a bad one throws `invalid_device_proof` or `device_account_revoked`. `db.getDeviceAccountProof()` now also answers on a Db opened by a session (`createJazzClient`, a provider), where it returned `null`.

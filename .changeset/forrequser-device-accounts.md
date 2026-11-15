---
"jazz-tools": patch
---

`forReqUser`, `withAttribution` and `accountFor` now act as a device account when `authenticate` returns `acceptDeviceAccounts(request)`: the request carries the account's proof from `db.getLocalFirstIdentityProof()` as `Authorization: Bearer`, checked as a sync connection's is (signature, expiry, audience, admission). Before, they threw `signed_out` for every device account. `db.getLocalFirstIdentityProof()` now also answers on a Db opened by a session (`createJazzClient`, a provider), where it returned `null`.

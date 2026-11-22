---
"jazz-tools": patch
---

**Device account names.** `db.getDeviceAccountProof()` is the new name of `db.getLocalFirstIdentityProof()`, and `DeviceAccountsAnswer` (from `jazz-tools/backend`, what `acceptDeviceAccounts` returns) the new name of `LocalFirstGuest`. The old names still work and are deprecated. `jazz-tools apps auth set` takes `--allow-device-accounts`; `--allow-local-first-auth` still works. Error messages, warnings and CLI output say "device account" where they said "local-first guest", "local-first secret" or "local-first auth".

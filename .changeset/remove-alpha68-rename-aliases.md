---
"jazz-tools": patch
---

**Breaking:** the alpha.68 rename aliases are removed. Use the names they pointed to:

| Removed                                          | Use instead                                           |
| ------------------------------------------------ | ----------------------------------------------------- |
| `db.getLocalFirstIdentityProof(options)`         | `db.getDeviceAccountProof(options)` (same options)    |
| type `LocalFirstGuest` from `jazz-tools/backend` | type `DeviceAccountsAnswer` from `jazz-tools/backend` |

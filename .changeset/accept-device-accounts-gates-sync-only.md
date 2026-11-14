---
"jazz-tools": patch
---

Docs: `acceptDeviceAccounts` says it gates device-account sync only. Linking a device account to a user who signs up (`linkDeviceAccount()`) is authorised by the backend's sign-in plus the device key's proof, so it works even when an app doesn't accept device accounts for sync.

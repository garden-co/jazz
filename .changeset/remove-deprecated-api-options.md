---
"jazz-tools": patch
---

**Breaking change (alpha):** Remove the deprecated `authSecretStorageKey` option from browser and Expo local-first auth. Migrate callers to `useLocalFirstAuth({ key: "..." })` (or the equivalent Expo hook option), and forward the returned `secret` to the client configuration. Remove the deprecated `permissions` option from `publishStoredSchema`; publish permissions separately with `publishStoredPermissions`.

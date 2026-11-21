---
"jazz-tools": minor
---

**Breaking:** the throwing stubs for the auth APIs alpha.61 removed are gone, as planned: they were kept for one release so a bundler named the right replacement. Importing one of these names now fails at build time ("not exported") instead of throwing `JAZZ_API_REMOVED` when called: `useJazzAuth`, `useJazzSessionOwner`, `useAccountState`, `createAccountState`, `createJazzSessionState`, `accountState`, `createAccountManager`, `AccountManager`, `createDb`, `createJazzApp`, `exportLocalFirstSecret`, `useLocalFirstAuth`, `createUseLocalFirstAuth`, `LocalFirstAuth`, `BrowserAuthSecretStore`, `ExpoAuthSecretStore`, `expoAuthSecretStore`, `JazzSessionProvider`, `JazzSvelteProvider` and `acceptLocalFirstAuth` (backend). What replaced each is listed in [Upgrading from alpha.60](https://jazz.tools/docs/operating/backups-and-upgrades#upgrading-from-alpha60).

Errors and docs that named the account manager's `loginWithBackend` or `linkWithBackend` now name the session's `refresh()` and `linkDeviceAccount()`.

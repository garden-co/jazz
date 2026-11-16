---
"jazz-tools": patch
---

`createJazzClient` from `jazz-tools/client` works on Node, Deno, Bun and CLIs without a `store`: where there is no browser storage (localStorage with Web Locks), the session keeps its selection in memory for the process, which is all a user your backend names (`bearer`) needs, so `createJazzClient({ serverUrl, bearer })` no longer throws "createAccountManager requires an AccountStore outside the browser". A device account on such a host (`initial: "device"`, `createDeviceAccount()`, `restoreDeviceAccount()`, `reopenDeviceAccount()`, `serverUrl: null`) still needs a `store` to keep its key, and is refused with a `DeviceAccountNeedsStoreError` naming that option. Store errors on browsers without localStorage or Web Locks, and on bare React Native, now name the `store` option of `createJazzClient`.

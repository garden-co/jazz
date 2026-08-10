# todo-client-localfirst-expo

Expo development-build example for the SQLite-backed `jazz-rn` runtime.
It exercises the public `jazz-tools/react-native` provider, local-first auth,
persistent reopen, pending-write replay, and live sync.

## Setup

- Expo Go is not supported; `jazz-rn` is a custom native module.
- Keep `jazz-rn` as a direct app dependency so React Native codegen discovers
  `JazzRnSpec` during prebuild.
- Import `jazz-tools/expo/polyfills` from the entry point before loading the app.
- `withExpoDataDirectory()` supplies Expo's absolute Documents directory and
  the native runtime stores `<dbName>.db` there. Production apps should decide
  whether that database belongs in device backups and apply the platform backup
  exclusion policy when it does not.
- `ExpoAuthSecretStore` persists the local-first identity with
  `expo-secure-store`.

```bash
pnpm --filter jazz-tools build:runtime
pnpm --filter jazz-rn ubrn:ios
pnpm --filter jazz-rn ubrn:android
pnpm --filter todo-client-localfirst-expo native:prebuild
pnpm --filter todo-client-localfirst-expo build
```

## iOS simulator E2E

The checked-in scripts use app id `00000000-0000-0000-0000-000000000002`,
port `1625`, and test-only credentials. Never ship the `EXPO_PUBLIC_JAZZ_E2E_*`
values.

First launch without a server and create the deterministic offline row:

```bash
EXPO_PUBLIC_JAZZ_E2E_SEED_TITLE=offline-seed \
EXPO_PUBLIC_JAZZ_E2E_SECRET=AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA \
pnpm --filter todo-client-localfirst-expo start
```

Terminate and relaunch the app process. The screen should change from
`E2E seed: created; rows: 1` to `E2E seed: reused; rows: 1`.

Then start the schema-initialized in-memory verifier server:

```bash
pnpm --filter todo-client-localfirst-expo e2e:server
```

Restart Metro with the server configuration:

```bash
EXPO_PUBLIC_JAZZ_E2E_SEED_TITLE=offline-seed \
EXPO_PUBLIC_JAZZ_E2E_SECRET=AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA \
EXPO_PUBLIC_JAZZ_E2E_ADMIN_SECRET=jazz-rn-e2e-admin \
EXPO_PUBLIC_JAZZ_SERVER_URL=http://127.0.0.1:1625 \
pnpm --filter todo-client-localfirst-expo start
```

The test server is provided by the NAPI build, which intentionally has no
compression codec. Match it when launching the iOS process:

```bash
SIMCTL_CHILD_JAZZ_TRANSPORT_COMPRESSION=none \
xcrun simctl launch booted dev.jazz.todo.localfirstexpo
```

Finally run the second client:

```bash
pnpm --filter todo-client-localfirst-expo e2e:sync-client
```

Success prints
`{"observedOfflineTitle":"offline-seed","insertedRemoteTitle":"remote-seed","rowCount":2}`
and the mobile UI shows both rows. Normal CLI-server/mobile deployments compile
zstd on both sides and do not need the simulator compression override.

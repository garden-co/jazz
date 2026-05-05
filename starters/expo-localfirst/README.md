# expo-localfirst

A minimal Expo starter for [Jazz](https://jazz.tools) with a pure local-first
todo app. Each device gets its own anonymous Jazz identity, persisted in the
device's secure storage via `expo-secure-store`.

## What this starter gives you

- A working Expo todo app that runs on first build, no configuration required.
- A local Jazz dev server started automatically by the `withJazz` Metro helper
  in `metro.config.mjs`.
- Row-level permissions wired through `$createdBy`, so every row is
  automatically scoped to the user who created it.
- A recovery-phrase backup widget so users can move their identity between
  devices or restore after clearing storage.

## Getting started

```bash
pnpm install
pnpm --filter expo-localfirst start
```

Then run on a device or simulator:

```bash
pnpm --filter expo-localfirst ios       # iOS simulator
pnpm --filter expo-localfirst android   # Android emulator
```

This app uses native code (`jazz-rn`), so it runs only as a development build
— **not** in Expo Go. The first run triggers `expo prebuild`.

No `.env` setup is required for the first run. The `withJazz` Metro helper
starts a local Jazz dev server and writes `EXPO_PUBLIC_JAZZ_APP_ID` and
`EXPO_PUBLIC_JAZZ_SERVER_URL` into `.env` on first start. Metro inlines them
into the bundle.

## Architecture

```
src/
  todo-widget.tsx                ← Jazz-powered todo list (React Native)
  auth-backup.tsx                ← recovery-phrase backup/restore
App.tsx                          ← app entry, mounts the Jazz provider
schema.ts                        ← Jazz app schema (todos table)
permissions.ts                   ← row-level access policy ($createdBy)
metro.config.mjs                 ← starts the Jazz dev server, injects env
```

## How it works

Every device gets its own Ed25519 secret, generated and stored by
`ExpoAuthSecretStore` (backed by `expo-secure-store`) on first launch. That
secret becomes the identity Jazz uses for all subsequent writes. `App.tsx`
calls `ExpoAuthSecretStore.getOrCreateSecret()` and hands the result to
`<JazzProvider>` as `secret`.

Data syncs to the Jazz server under that anonymous identity. There is no
concept of a user account, no sign-in, no sign-out — the device _is_ the
account. To move data between devices, use the recovery-phrase widget to
export the secret as a 24-word BIP39 phrase and restore it on the new
device.

## Extending the schema

Edit `schema.ts` to add tables. The Jazz dev server watches the file and
republishes the schema on change.

```ts
const schema = {
  todos: s.table({ title: s.string(), done: s.boolean() }),
  projects: s.table({ name: s.string() }),
};
```

Row ownership is enforced by `permissions.ts` via the `$createdBy` predicate,
so you don't need an explicit `ownerId` column. Jazz records the creating
session on every row and the permission policy scopes reads/writes to it.

## Environment variables

| Variable                      | When       | Source                                                |
| ----------------------------- | ---------- | ----------------------------------------------------- |
| `EXPO_PUBLIC_JAZZ_APP_ID`     | cloud only | scaffolder (`create-jazz --hosting hosted`) or manual |
| `EXPO_PUBLIC_JAZZ_SERVER_URL` | cloud only | scaffolder or manual                                  |
| `JAZZ_ADMIN_SECRET`           | cloud only | scaffolder or manual                                  |
| `BACKEND_SECRET`              | cloud only | scaffolder or manual                                  |

Leave all four unset for self-hosted mode — the `withJazz` Metro helper
spawns a local Jazz dev server and writes
`EXPO_PUBLIC_JAZZ_APP_ID` / `EXPO_PUBLIC_JAZZ_SERVER_URL` into `.env` on
first start. For cloud mode, either scaffold via
`create-jazz --hosting hosted` or provision an app at
https://v2.dashboard.jazz.tools and paste the four values into `.env`.

Server URL defaults when running locally:

- iOS simulator: `http://127.0.0.1:1625`
- Android emulator: `http://10.0.2.2:1625`
- Physical device: `http://<your-lan-ip>:1625`

## Deploying to production

For cloud-hosted deployments, set the four env vars above in your build
environment (EAS / your CI) and your app will sync against Jazz Cloud.

For self-hosted deployments you need to run your own Jazz server. The server
requires `--allow-local-first-auth` explicitly in production:
`jazz-tools server <APP_ID> --allow-local-first-auth`. Without it,
anonymous local-first connections will receive auth errors.

## Known limitations

- **One device per identity by default.** The secret lives in the device's
  secure storage; uninstalling the app or wiping data discards the identity.
  Use the recovery-phrase widget to move the identity between devices.
- **No native passkey backup.** The recovery-phrase flow is the only
  built-in backup option on Expo. Browser passkeys (`BrowserPasskeyBackup`)
  are not available in React Native.

## Where to go next

- `schema.ts` and `permissions.ts` — the two files you'll touch most when
  extending the starter.
- `src/todo-widget.tsx` — replace with your own UI; the Jazz hooks
  (`useDb`, `useAll`) work the same regardless of component.

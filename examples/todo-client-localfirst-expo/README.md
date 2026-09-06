# todo-client-localfirst-expo

Expo local-first todos using `jazz-tools/react-native` and the installed `jazz-rn` runtime.

Account preparation happens outside the database context. The app creates or restores a local-first `AccountHandle`, keeps its recovery material in Expo SecureStore, and passes the handle to an effect-owned client. The shared `jazz-tools/expo` adapter uses the native account-store lock to preserve recovery roots across overlapping JavaScript runtimes.

## Notes

- This app uses native code (`jazz-rn`), so use a development build (`expo run:ios` / `expo run:android`).
- It does **not** run in Expo Go.
- Keep `jazz-rn` as a **direct app dependency** so React Native codegen discovers `JazzRelaySpec` during prebuild.
- Add `"plugins": ["jazz-rn"]` to your Expo config. The plugin enables the New Architecture required by the TurboModule; run `expo prebuild` and create a development build after adding it.
- The example keeps `jazz-rn: "workspace:*"` because it is developed inside this
  repository. An adopter copies the same `app.json` shape but installs
  `jazz-rn@alpha` directly, as described in the package README.
- Install a matching `jazz-rn` native build; account-scoped persistent relay admission is handled by the runtime.
- Start a Jazz server first (for example: `jazz-tools server <APP_ID> --port 1625`).
- Server URL defaults:
  - iOS simulator: `http://127.0.0.1:1625`
  - Android emulator: `http://10.0.2.2:1625`
  - Physical device: `http://<your-lan-ip>:1625`
- Set both `EXPO_PUBLIC_JAZZ_APP_ID` and a device-reachable `EXPO_PUBLIC_JAZZ_SERVER_URL` before starting Metro.
- Auth uses `createAccountManager` from `jazz-tools/expo`, with app/server-scoped Expo SecureStore persistence.
- Todos carry `owner_id`, and mutations are authorized against `session.user.account`; ownership columns use UUIDs.

## Commands

```bash
pnpm install
pnpm --filter todo-client-localfirst-expo verify:expo
pnpm --filter jazz-tools build
pnpm --filter todo-client-localfirst-expo build
pnpm --filter todo-client-localfirst-expo start
```

`verify:expo` checks clean Android and iOS prebuild/autolink configuration; it
does not execute device acceptance tests. After a matching native artifact package is installed, use
`pnpm --filter todo-client-localfirst-expo android` or `ios` to rebuild the
development app.

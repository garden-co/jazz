# todo-client-localfirst-expo

Expo local-first todos using `jazz-tools/react-native` and the installed `jazz-rn` runtime.

`<JazzProvider config={{ serverUrl, initial: "device" }}>` owns the session: it starts (or reopens) a device account, whose key it keeps in Expo SecureStore, and the native client for it. The shared `jazz-tools/expo` adapter uses the native account-store lock to preserve the device account across overlapping JavaScript runtimes.

## Notes

- This app uses native code (`jazz-rn`), so use a development build (`expo run:ios` / `expo run:android`).
- It does **not** run in Expo Go.
- Keep `jazz-rn` as a **direct app dependency** so React Native codegen discovers `JazzRelaySpec` during prebuild.
- Add `"plugins": ["jazz-rn"]` to your Expo config. The plugin enables the New Architecture required by the TurboModule; run `expo prebuild` and create a development build after adding it.
- The example keeps `jazz-rn: "workspace:*"` because it is developed inside this
  repository. An adopter copies the same `app.json` shape but installs
  `jazz-rn` from npm directly, as described in the package README.
- Install a matching `jazz-rn` native build; account-scoped persistent relay admission is handled by the runtime.
- Start the app's backend first: `pnpm backend` (`server.ts` runs `createJazz` and serves sync under `/jazz` on port 1625; data stays in `.jazz/`).
- Set `EXPO_PUBLIC_JAZZ_SERVER_URL` to the backend's `/jazz` URL as the device reaches it before starting Metro:
  - iOS simulator: `http://127.0.0.1:1625/jazz`
  - Android emulator: `http://10.0.2.2:1625/jazz`
- Physical devices: either reach the backend on your machine's LAN IP (plain HTTP; the device must be on
  the same network and allow cleartext traffic), or deploy the backend behind a device-trusted HTTPS endpoint.
- `JazzProvider` from `jazz-tools/expo` owns the session: it starts a device account (`initial: "device"`) and the native client, with server-scoped Expo SecureStore persistence.
- Todos carry `owner_id`, and mutations are authorized against `session.user`; ownership columns use UUIDs.

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

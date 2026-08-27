# todo-client-localfirst-expo

Expo example scaffolding for local-first todos using `jazz-tools/react` + `jazz-rn`.

> **Alpha boundary:** this is compile/build scaffolding, not a runnable persistent Jazz client.
> The SQLite and native-relay Rust foundations exist, but are not connected to
> `jazz-rn`; memory mode has not been validated under Metro/Hermes on a device.

## Notes

- This app uses native code (`jazz-rn`), so use a development build (`expo run:ios` / `expo run:android`).
- It does **not** run in Expo Go.
- Keep `jazz-rn` as a **direct app dependency** so React Native codegen discovers `JazzRelaySpec` during prebuild.
- Add `"plugins": ["jazz-rn"]` to your Expo config. The plugin enables the New Architecture required by the TurboModule; run `expo prebuild` and create a development build after adding it.
- RN storage is not wired up yet; do not rely on persistence or a `dataPath`
  option until the native relay binding is implemented.
- Start a Jazz server first (for example: `jazz-tools server <APP_ID> --port 1625`).
- Server URL defaults:
  - iOS simulator: `http://127.0.0.1:1625`
  - Android emulator: `http://10.0.2.2:1625`
  - Physical device: `http://<your-lan-ip>:1625`
- If you set `EXPO_PUBLIC_JAZZ_SERVER_URL` to `localhost`/`127.0.0.1`, the app now rewrites it in dev when needed so devices can still reach your host machine.
- Auth uses local-first identity via `ExpoAuthSecretStore` (backed by `expo-secure-store`).
- Todos carry `owner_id`, and mutations are authorized against `session.user`.

## Commands

```bash
pnpm --filter jazz-tools build
pnpm --filter todo-client-localfirst-expo build
pnpm --filter todo-client-localfirst-expo verify:expo:android
pnpm --filter todo-client-localfirst-expo start
```

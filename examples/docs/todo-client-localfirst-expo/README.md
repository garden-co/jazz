# todo-client-localfirst-expo

Expo example for local-first todos using `jazz-tools/react-native` + `jazz-rn`.

> **Alpha boundary:** this is compile/build scaffolding, not a runnable persistent Jazz client.
> React Native SQLite persistence is unavailable, and memory mode has not been validated under
> Metro/Hermes on a device.

## Notes

- This app uses native code (`jazz-rn`), so use a development build (`expo run:ios` / `expo run:android`).
- It does **not** run in Expo Go.
- Keep `jazz-rn` as a **direct app dependency** so React Native codegen discovers `JazzRelaySpec` during prebuild.
- RN storage is not wired up yet; do not rely on persistence or a `dataPath`
  option until the native relay binding is implemented.
- Start the app's backend first: `pnpm backend` (`server.ts` runs `createJazz` and serves sync under `/jazz` on port 1625; data stays in `.jazz/`).
- Server URL defaults for local development:
  - iOS simulator: `http://127.0.0.1:1625/jazz`
  - Android emulator: `http://10.0.2.2:1625/jazz`
- Physical devices: either reach the backend on your machine's LAN IP (plain HTTP; the device must be on
  the same network and allow cleartext traffic), or deploy the backend behind a device-trusted HTTPS endpoint.
- Set `EXPO_PUBLIC_JAZZ_SERVER_URL` to a device-reachable `/jazz` URL of the backend otherwise. The Expo adapter does not rewrite loopback HTTP URLs into a device endpoint.
- Account preparation uses `createAccountManager` from `jazz-tools/expo`, backed by SecureStore with native atomic updates. Contexts receive an opaque account handle. A matching native development build is required; Expo Go is unsupported.
- Todos carry `owner_id`, and mutations are authorized against `session.user`.

## Commands

```bash
pnpm --filter jazz-tools build
pnpm --filter todo-client-localfirst-expo build
pnpm --filter todo-client-localfirst-expo verify:expo:android
pnpm --filter todo-client-localfirst-expo start
```

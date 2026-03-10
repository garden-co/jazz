# todo-client-localfirst-expo

Expo example for local-first todos using `jazz-tools/react-native` + `jazz-rn`.

## Notes

- This app uses native code (`jazz-rn`), so use a development build (`expo run:ios` / `expo run:android`).
- It does **not** run in Expo Go.
- Keep `jazz-rn` as a **direct app dependency** so React Native codegen discovers `JazzRnSpec` during prebuild.
- RN storage is Fjall-backed. You can optionally pass `dataPath` in `JazzProvider` config to pick a specific file path.
- Start a Jazz server first (for example: `jazz-tools server <APP_ID> --port 1625`).
- Server URL defaults:
  - iOS simulator: `http://127.0.0.1:1625`
  - Android emulator: `http://10.0.2.2:1625`
  - Physical device: `http://<your-lan-ip>:1625`
- If you set `EXPO_PUBLIC_JAZZ_SERVER_URL` to `localhost`/`127.0.0.1`, the app now rewrites it in dev when needed so devices can still reach your host machine.
- Auth now matches the browser local-first examples: default local mode is `demo`, todos carry `owner_id`, and mutations are authorized against `session.user_id`.
- You can override auth identity with `EXPO_PUBLIC_JAZZ_LOCAL_MODE` (`demo` or `anonymous`) and `EXPO_PUBLIC_JAZZ_LOCAL_TOKEN`.

## Commands

```bash
pnpm --filter jazz-tools build
pnpm --filter todo-client-localfirst-expo build
pnpm --filter todo-client-localfirst-expo verify:expo:android
pnpm --filter todo-client-localfirst-expo start
```

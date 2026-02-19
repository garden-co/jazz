# todo-client-localfirst-expo

Expo example for local-first todos using `jazz-tools/react-native` + `jazz-rn`.

## Notes

- This app uses native code (`jazz-rn`), so use a development build (`expo run:ios` / `expo run:android`).
- It does **not** run in Expo Go.
- Keep `jazz-rn` as a **direct app dependency** so React Native codegen discovers `JazzRnSpec` during prebuild.
- RN storage is SurrealKV-backed. You can optionally pass `dataPath` in `JazzProvider` config to pick a specific file path.

## Commands

```bash
pnpm --filter jazz-tools build
pnpm --filter todo-client-localfirst-expo build
pnpm --filter todo-client-localfirst-expo verify:expo:android
pnpm --filter todo-client-localfirst-expo start
```

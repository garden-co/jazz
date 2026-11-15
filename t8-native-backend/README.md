t8-native-backend: security gate for the .65 native `/jazz` endpoint (Rust `jazz-app` / Go), if it ships.
- `npx tsx jwks.ts` — local JWKS + token mint (valid / expired / wrong-iss / bad-sig / no-sub); point the backend's JWT issuer/JWKS at it.
- `probe-auth.ts` — item 1 (refusals), HTTP.
- `probe-frames.ts` — item 4 (malformed / oversized frames don't crash; server still answers).
- Items 2–3 (permissions, second user can't read private rows): `smoke-stageplan.ts`, written once the StagePlan app files + start commands are known (asked #jazz-app-backend-handler and #go).

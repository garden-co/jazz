t8-native-backend: security gate for the .65 native `/jazz` endpoint (Rust `jazz-app` / Go), if it ships.
- `npx tsx jwks.ts` — token mint: RS256 + JWKS by default; with `JAZZ_JWT_SECRET` (+ `JAZZ_JWT_ISSUER`) set, HS256 for the Rust example. Kinds: valid / expired / wrong-iss / bad-sig / no-sub; `claims` query param adds a `claims` object.
- `probe-auth.ts` — item 1 (refusals), HTTP.
- `probe-frames.ts` — item 4: junk/empty/truncated first frames, an oversized (>64 KiB) prelude, and with `PRELUDE=<json>` a junk frame and a 2 MiB + 1 frame after a valid prelude (expect 1009); server must still answer after each.
- `probe-raw.ts` — item 4, raw RFC 6455 frames `ws` cannot send (unmasked, RSV bits, unknown opcode, oversized/fragmented control, stray continuation, text, bad close code, 64-bit length just over the cap; default CAP 2 MiB = Jazz sync-message limit): expects the documented close codes and a live `HEALTH` after each.
- Items 2–3 (permissions, second user can't read private rows): `smoke-stageplan.ts`, written once the StagePlan app files + start commands are known (asked #jazz-app-backend-handler and #go).

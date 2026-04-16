# Missing E2E test: JazzClient.updateAuthToken reaches live Rust transport

## What

Current tests for `JazzClient.updateAuthToken` (`packages/jazz-tools/src/runtime/client.test.ts`) stub the Runtime object and only assert the JSON payload shape. No integration test exercises the full path `updateAuthToken → runtime.updateAuth → TransportControl::UpdateAuth → manager reconnect with refreshed auth` against a real NAPI or WASM runtime and server. Without it, a regression that breaks the roundtrip on any one binding (the RN-adapter-missing-updateAuth bug we just fixed) would pass unit tests silently.

## Priority

high

## Notes

- Add integration coverage against at least NAPI (fast CI path) and the browser worker path.
- Scenario: connect with invalid JWT → server rejects with `Unauthorized` → `onAuthFailure` fires → `updateAuthToken(newJwt)` → reconnect succeeds → sync resumes.
- Existing `napi.auth-failure.test.ts` covers the failure notification but not the recovery via `updateAuthToken`.

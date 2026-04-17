# Expo Android Maestro E2E does not see any `ws client connected` logs

## What

`.github/workflows/expo-android-maestro-e2e.yml` runs the Expo app against a `jazz-tools server` inside a Vercel Sandbox, then fails at the "Verify server evidence" step because `grep -c 'ws client connected' server.log` returns 0. Suspected root cause: the Vercel Sandbox public-port proxy may not forward WebSocket Upgrade requests. `/health` (plain HTTP) works; WS upgrades appear to be dropped or rejected at the edge before reaching `jazz-tools server`.

## Priority

medium

## Notes

- Verify: add a step that does `curl -i -H "Connection: Upgrade" -H "Upgrade: websocket" -H "Sec-WebSocket-Version: 13" -H "Sec-WebSocket-Key: ..." "$SERVER_PUBLIC_URL/ws"` and inspect the status code.
  - 101 → proxy is fine; the problem is client-side on the RN build.
  - 400/404/502 → Vercel Sandbox does not proxy WS; switch provider or terminate WS differently.
- Also add `tracing::debug!("ws upgrade received")` at the top of `handle_ws_connection` so failed handshakes leave a trace.
- Not a merge blocker for the auth-refresh PR — local browser + NAPI E2E succeed.

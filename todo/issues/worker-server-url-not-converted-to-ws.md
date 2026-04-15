# Convert worker server URL before opening Rust WS transport

## What

`runtime.connect` is called with `msg.serverUrl` verbatim, but worker init receives the app-level `serverUrl`/`serverPathPrefix` config (typically HTTP + optional prefix). Passing raw `http(s)` URLs (or ignoring `serverPathPrefix`) causes the Rust WebSocket transport to dial the wrong endpoint, so worker upstream sync never attaches in those deployments. The worker should normalize to the `/ws` URL the same way `httpUrlToWs` is used elsewhere.

## Priority

high

## Notes

# HTTP/SSE Transport — TODO

Remaining work items and future enhancements.

> Status quo: [specs/status-quo/http_transport.md](../../status-quo/http_transport.md)

## Phasing

- **MVP**: JWKS support, server-side error events
- **Launch**: Exponential backoff
- **Later**: WebSocket alternative, request signing, CORS configuration

## MVP: JWKS Support

Currently only HMAC secret for JWT validation. JWKS (JSON Web Key Sets) needed for production with key rotation and caching.

> `crates/jazz-cli/src/middleware/auth.rs:250` — TODO comment

## MVP: Server-Side Error Events

`ServerEvent::Error` type exists in the spec but is never sent by the server. Errors only reported via HTTP status codes on sync requests. Should also push errors on the event stream for client-side debugging.

## Launch: Exponential Backoff

Reconnection uses fixed 5s delay. Should implement exponential backoff with jitter.

> `crates/jazz-rs/src/client.rs:252`

## Later: Future Enhancements

- WebSocket alternative to SSE
- Request signing/verification
- CORS configuration beyond permissive dev mode

Note: binary protocol option is covered by `unified_binary_sync_protocol.md`.
